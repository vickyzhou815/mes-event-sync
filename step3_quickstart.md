# Step 3 — Redis cache-aside, slow consumer, Prometheus + Grafana

## Goal

1. Add Redis cache-aside layer — RTD consumer writes to Redis after each
   MySQL upsert, so status reads hit Redis first instead of MySQL directly.
2. Add a deliberately slow consumer — makes Kafka lag visible and
   measurable, proving the backpressure story: Kafka absorbs burst traffic
   durably while slow consumers catch up at their own pace.
3. Add Prometheus + Grafana — visualizes consumer group lag as a real
   time-series dashboard, the same way production teams monitor Kafka health.

---

## Concepts clarified before building

**Q: Events to Redis — from Kafka consumer or from MySQL?**
From the Kafka consumer (Java), not from MySQL directly. The RTD consumer
reads a Kafka message, writes to MySQL, then also writes to Redis — both
writes happen inside the same Java processing loop, from the same event.
MySQL never touches Redis directly.

**Q: What is Kafka lag exactly?**
Lag = latest offset in topic minus last offset this consumer processed.
In plain terms: how many messages exist in the topic that a specific
consumer group has not yet processed. During normal traffic, lag hovers
near zero. During a burst, lag grows as messages arrive faster than the
consumer processes them. After the burst, the consumer drains the backlog
and lag shrinks back toward zero. Nothing is lost at any point — Kafka
holds every message until the consumer gets to it.

**Q: What tool visualizes lag in real production?**
Most common options:
- Kafka UI — simple web dashboard showing consumer group lag as a table
- Prometheus + Grafana — scrapes Kafka metrics via kafka-exporter,
  graphs lag as a time-series. This is what we built here, and connects
  directly to the lot-tracking FastAPI project's existing Prometheus setup.
- Confluent Control Center — enterprise version with alerting built in
- Datadog / New Relic — commercial APM tools used at large scale

Real teams set alert rules like "if lag on order-fulfillment-consumer-group
exceeds 10,000 messages for 5 minutes, page the on-call engineer."

**Q: Does the Grafana dashboard use the lag calculated in SlowConsumer.java?**
No — two separate measurements of the same phenomenon:
- Grafana shows kafka_consumergroup_lag in units of number of messages,
  from Kafka's own internal bookkeeping exported via kafka-exporter.
- SlowConsumer.java calculates lag in milliseconds of real time, by
  comparing each message's production timestamp to the current time.
  This appears in the terminal log output only.
Both complement each other: message count tells you backlog size, time
lag tells you how stale your data is getting.

---

## New infrastructure added

Updated `docker-compose.yml` to add four new services:

```yaml
  redis:
    image: redis:7.2-alpine
    ports:
      - "6379:6379"

  kafka-exporter:
    image: danielqsj/kafka-exporter:latest
    depends_on:
      - kafka
    ports:
      - "9308:9308"
    command:
      - --kafka.server=kafka:29092

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./docker/prometheus.yml:/etc/prometheus/prometheus.yml
    depends_on:
      - kafka-exporter

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: practice
    depends_on:
      - prometheus
```

Created `docker/prometheus.yml`:

```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: kafka-exporter
    static_configs:
      - targets:
          - kafka-exporter:9308
```

`scrape_interval: 5s` — Prometheus polls every 5 seconds, fine-grained
enough to see lag building and draining during a burst in near real time.
`kafka-exporter:9308` — reached by container name, same pattern as
Debezium reaching MySQL by hostname.

Started the new containers:

```bash
docker compose up -d redis kafka-exporter prometheus grafana
```

Verified all containers running:

```bash
docker compose ps
```

Verified Prometheus and Grafana healthy:

```bash
curl http://localhost:9090/-/ready
curl http://localhost:3000/api/health
```

Verified Prometheus receiving Kafka metrics — opened in browser:
http://localhost:9090/targets

kafka-exporter showed green UP status.

Verified lag metrics flowing:
http://localhost:9090/graph?g0.expr=kafka_consumergroup_lag&g0.tab=1

Showed one row per consumer group with current lag values.

---

## Grafana dashboard setup

1. Opened http://localhost:3000, logged in (admin / practice)
2. Connections -> Data sources -> Add data source -> Prometheus
3. URL: http://prometheus:9090 -> Save & test
4. Dashboards -> New -> New dashboard -> Add visualization
5. Query: kafka_consumergroup_lag{topic="mes.mes_db.lot_status"}
6. Panel title: Consumer Group Lag
7. Applied, saved dashboard as "MES Event Sync"
8. Set auto-refresh to 5s, time window to Last 5 minutes

---

## New files added

- `consumers/pom.xml` — added Jedis (Redis Java client) dependency
- `consumers/src/main/java/com/practice/consumers/RtdConsumer.java`
  — updated to also write to Redis after each MySQL upsert
- `consumers/src/main/java/com/practice/consumers/SlowConsumer.java`
  — new consumer, 500ms sleep per message, logs real-time lag in ms

---

## Code changes: RtdConsumer.java

Added Jedis import and two new additions to the constructor and run loop:

Key Redis write method added:

```java
private void writeToRedis(Map<String, String> after) {
    String lotId = after.get("lot_id");
    String state = after.get("state");
    String toolId = after.get("tool_id");
    String key = "lot:" + lotId;
    String value = "state=" + state + ",tool=" + toolId;
    jedis.setex(key, REDIS_TTL_SECONDS, value);
    System.out.println("[RTD consumer] Redis cached lot=" + lotId + " -> " + value);
}
```

`jedis.setex` means "set with expiry" — key auto-expires after 3600
seconds (TTL). This is the self-healing safety net: even if a Redis write
was missed, the stale cache entry disappears on its own eventually and
the next read fetches fresh data from MySQL.

Key naming convention `lot:{lot_id}` uses a colon as namespace separator
— standard Redis practice to keep keys organized when many different
data types share the same Redis instance.

---

## Building and running

```bash
cd consumers
mvn clean package
```

Run each in its own terminal tab:

```bash
# Tab 1 — simulator (from simulator/ folder)
java -cp target/lot-simulator-1.0.0.jar com.practice.simulator.LotStatusSimulator jdbc:mysql://localhost:3306/mes_db root practice

# Tab 2 — RTD consumer (from consumers/ folder)
java -cp target/lot-consumers-1.0.0.jar com.practice.consumers.RtdConsumer

# Tab 3 — ERP consumer
java -cp target/lot-consumers-1.0.0.jar com.practice.consumers.ErpConsumer

# Tab 4 — slow consumer
java -cp target/lot-consumers-1.0.0.jar com.practice.consumers.SlowConsumer
```

---

## Verification

**RTD consumer confirmed writing to both MySQL and Redis:**
[RTD consumer] MySQL upserted lot=LOT-00065 state=scrap
[RTD consumer] Redis cached lot=LOT-00065 -> state=scrap,tool=TOOL-17

**Redis confirmed holding cached lot status:**

```bash
docker exec -it mes-event-sync-redis-1 redis-cli
GET lot:LOT-00065
# returns: "state=scrap,tool=TOOL-17"
SCAN 0 MATCH lot:* COUNT 20
# returns list of lot:LOT-XXXXX keys
exit
```

**Slow consumer confirmed logging real-time lag:**
[Slow consumer] processing lot=LOT-00025 state=in_process lag=76402ms
[Slow consumer] processing lot=LOT-00130 state=complete lag=76907ms

Lag growing by ~500ms per message — exactly matching the Thread.sleep(500)
delay, confirming it is genuinely falling behind the incoming event rate.

---

## Grafana dashboard result

Three lines visible on the Consumer Group Lag panel:
- erp-consumer-group (green) — near zero throughout, keeping up fine
- rtd-consumer-group (yellow) — near zero throughout, keeping up fine
- slow-consumer-group (blue) — climbed from ~100 to ~700 during burst
  windows, with partial draining between bursts

**What this proves**

Three independent consumers read from the same Kafka topic. Two of them
(RTD and ERP) maintained near-zero lag even during burst periods. The
third, simulating a slower downstream system like a reporting pipeline,
fell behind during bursts — lag climbed to ~700 messages at peak.
Critically, nothing crashed and nothing was lost. Kafka held the backlog
durably, and the slow consumer drained it at its own pace between bursts.

This is the core architectural guarantee during a Black-Friday-style
traffic spike: fast consumers are never blocked by slow ones, and no
messages are dropped regardless of processing speed difference. This
directly mirrors the real fab scenario where downstream systems like
yield analysis consumed lot events at their own pace, independently of
how fast equipment events were arriving.
