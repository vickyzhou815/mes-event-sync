# Step 2 summary — independent consumers, MySQL + MongoDB fan-out

## Goal

Two independent Java consumers read from the same Kafka topic
(`mes.mes_db.lot_status`), simulating the RTD and ERP systems. Each writes
to its own store: RTD-style consumer to MySQL, ERP-style consumer to
MongoDB. Both are idempotent on `lot_id`, so duplicate Kafka deliveries
never produce duplicate or corrupt records.

## New files added

- `consumers/pom.xml` — Maven build file, adds the Kafka client, MongoDB
  driver, and Jackson (JSON parsing) dependencies.
- `consumers/src/main/java/com/practice/consumers/DebeziumEventParser.java`
  — shared helper, extracts the `payload.after` fields from a raw Debezium
  change event, used by both consumers.
- `consumers/src/main/java/com/practice/consumers/RtdConsumer.java` —
  reads the topic, upserts into a new MySQL table `lot_status_rtd`.
- `consumers/src/main/java/com/practice/consumers/ErpConsumer.java` —
  reads the same topic, upserts into a MongoDB collection
  `erp_db.lot_status_erp`.

## Schema change

Added to `schemas/mysql_init.sql`:

```sql
CREATE TABLE IF NOT EXISTS lot_status_rtd (
    lot_id      VARCHAR(32) PRIMARY KEY,
    state       VARCHAR(16) NOT NULL,
    tool_id     VARCHAR(32),
    synced_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

Since MySQL was already running earlier, this had to be applied manually
to the live container rather than relying on the init script:

```bash
docker exec -i mes-event-sync-mysql-1 mysql -uroot -ppractice mes_db < schemas/mysql_init.sql
```

Verified with:

```bash
docker exec -it mes-event-sync-mysql-1 mysql -uroot -ppractice mes_db -e "SHOW TABLES;"
```

Expected both `lot_status` and `lot_status_rtd` listed.

## Bringing up MongoDB

```bash
docker compose up -d mongo
```

Verified with `docker compose ps`, expected `mongo` row showing `Up`.

## Building and running the consumers

```bash
cd consumers
mvn clean package
```

Run each in its own terminal tab:

```bash
java -cp target/lot-consumers-1.0.0.jar com.practice.consumers.RtdConsumer
java -cp target/lot-consumers-1.0.0.jar com.practice.consumers.ErpConsumer
```

Each subscribes to `mes.mes_db.lot_status` under its own consumer group
(`rtd-consumer-group`, `erp-consumer-group`) — separate group IDs are what
make their offset tracking fully independent of each other.

## Verifying the data landed correctly

MySQL (RTD side):

```bash
docker exec -it mes-event-sync-mysql-1 mysql -uroot -ppractice mes_db -e "SELECT * FROM lot_status_rtd LIMIT 5;"
```

MongoDB (ERP side):

```bash
docker exec -it mes-event-sync-mongo-1 mongosh erp_db
```

then inside the mongosh shell:

```javascript
db.lot_status_erp.find().limit(5)
```

Both showed correctly normalized, de-duplicated records — one document/row
per `lot_id`, confirming the idempotent upsert logic works even though the
simulator repeatedly updates the same pool of 200 lots.

## The schema-evolution test (the main point )

Added a new column directly to the source table, with both consumers and
the CDC pipeline still running:

```bash
docker exec -it mes-event-sync-mysql-1 mysql -uroot -ppractice mes_db -e "ALTER TABLE lot_status ADD COLUMN priority VARCHAR(16) DEFAULT 'normal';"
```

Checked the very next Kafka message:

```bash
docker exec -it mes-event-sync-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic mes.mes_db.lot_status --max-messages 1
```

Result: the `priority` field appeared automatically in `payload.after`,
with no changes to the Debezium connector config, no restart, and no
manual registration step.

## What this proves

- Schema changes upstream are automatically visible downstream through the
  Kafka stream, the moment they happen, with zero manual registration —
  directly contrasting with the old DB2-to-Oracle/RTD setup, where a new
  table or field needed explicit infra-team configuration before
  downstream systems could see it, and a missed step caused silent,
  undetected staleness.
- Existing consumers did not crash or error when the new field appeared,
  because they simply don't reference it yet — schema evolution is
  non-breaking by default. The data is visible in the stream regardless
  of whether any given consumer currently chooses to use it.
- This is a more precise framing than "it has no effect downstream" — the
  new field is fully present and available to any consumer that wants it;
  existing consumers are just not forced to change.

