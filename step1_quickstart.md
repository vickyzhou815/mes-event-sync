## 1. Start the infrastructure
```
docker compose up -d
```
This starts zookeeper, kafka, connect, mysql, mongo, and redis. Give it 30-60 seconds, Kafka Connect takes the longest to come up.

be more specific:
 - docker compose up -d zookeeper kafka
   Kafka needs somewhere to store a small amount of coordination information — things like "which servers exist in this Kafka cluster," "who's currently the leader for this topic," that kind of bookkeeping. Historically, Kafka used a separate tool called Zookeeper to store and manage that coordination data. So in older Kafka setups (and the Docker images we're using), Zookeeper is a required companion service that runs alongside Kafka — Kafka itself doesn't store that bookkeeping internally, it asks Zookeeper. (older versions)
 - docker compose up -d mysql  (stop local mysql first, if running)
   then check: docker exec -it mes-event-sync-mysql-1 mysql -uroot -ppractice mes_db -e "SHOW TABLES; SELECT User, Host FROM mysql.user WHERE User='debezium';"
 - docker compose up -d connect
   then check: curl http://localhost:8083/
   Port 8083 is Kafka Connect's own REST API — it's how you (or a script, like the curl command we just ran) talk to Kafka Connect itself, to ask it things like "what connectors are registered," "register this new connector," "what's the status of this connector." It's a management/control interface, not part of the actual data flow.
   ("8083:8083" in docker-compose.yml:  means: "take port 8083 inside the container, and make it reachable as port 8083 on my Mac too.)
   (KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092: This is Kafka exposing two different addresses for two different audiences: kafka:29092 is for other containers (like Connect) talking to it over Docker's internal network, using the container's name (kafka) as the hostname — that only works between containers, not from Mac. localhost:9092 is for things running directly on Mac (like the console consumer command, or if ever connected a local Java program straight to Kafka) — and that's also mapped via "9092:9092" in the ports section, the same doorway pattern as 8083.)

Check all containers are running:
```
docker compose ps
```
Every service should show "Up" or "running" in the status column.


## 2. Register the Debezium connector
```
curl -X POST -H "Content-Type: application/json"
--data @connectors/lot-status-connector.json 
http://localhost:8083/connectors
```

Then check it is actually running:
```
curl http://localhost:8083/connectors/lot-status-connector/status
```
Look for "state": "RUNNING" in the response. If it says "FAILED" instead, read the error message in that same response, it usually tells exactly what went wrong.


## 3. Build and run the simulator
```
cd simulator
mvn clean package
java -cp target/lot-simulator-1.0.0.jar com.practice.simulator.LotStatusSimulator jdbc:mysql://localhost:3306/mes_db root practice
```

Should see lines like: lot=LOT-00042 -> state=complete tool=TOOL-07
scrolling by, with a burst window roughly every 45-60 seconds.

(mvn clean package actually produces: a file at simulator/target/lot-simulator-1.0.0.jar — that's the "build output," a single runnable Java program with the MySQL driver bundled inside it. That's the exact file java -cp target/lot-simulator-1.0.0.jar ... command is running right now.)

## 4. Watch the CDC events arrive in Kafka

In a new terminal tab:
```
docker exec -it mes-event-sync-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic mes.mes_db.lot_status --from-beginning
```
If that container name does not match, run docker ps first to find the exact name of the Kafka container.


## What done looks like

The connector status shows RUNNING, the simulator is steadily printing updates
with visible bursts, and the Kafka topic shows a live stream of change events
that match what the simulator is doing.

## If something goes wrong

- Connector will not register, or connection refused: Kafka Connect needs Kafka
  to be fully up first. Run docker compose logs connect to see what it is
  waiting on.
- Connector status shows FAILED with a privilege error: check that the GRANT
  statement in schemas/mysql_init.sql actually ran. Run docker compose logs mysql
  to see if it executed on first startup. If you already ran docker compose up
  once before, the init script will not re-run on a second start, run
  docker compose down -v first to wipe the old data and start clean.
- No messages in the topic at all: confirm the simulator printed "Connected to
  MySQL" and double check table.include.list in the connector config matches
  mes_db.lot_status exactly.
