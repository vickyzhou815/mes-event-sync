# MES Event Sync, Rebuilt

A practice project rebuilding a real architectural pattern from a 300mm wafer fab MES environment using a modern CDC/streaming stack: Debezium, Kafka, MySQL, MongoDB, Redis, Java, and PHP/Laravel.

## Why this project exists

At a previous job, lot-state changes (start, complete, hold, scrap) were written to a DB2 MES database, then needed to reach two downstream systems: an RTD system and an ERP system. That sync relied on manually configured replication/mapping — new tables had to be explicitly registered before Oracle could see their data, and RTD relied on a separately-configured lookup/mapping table. When registration was missed, downstream systems silently had stale or missing data — a class of inconsistency I personally had to diagnose.

This project rebuilds that shape with modern tooling I haven't used professionally (Debezium, Kafka, MongoDB, Redis, Laravel), specifically to see how schema-aware CDC and event streaming address the failure mode I encountered: schema changes should be visible in the pipeline, not silently dropped.

## Architecture

        lot_status table (MySQL)  <- simulates the MES/DB2 source of truth
                |
        Debezium (CDC, watches MySQL binlog)
                |
        Kafka topic: lot_events
                |
    ------------+----------
    |                     |
    v                     v
 RTD consumer          ERP consumer
 (Java->MySQL)        (Java->MongoDB)
    |                     |
    v                     v
   MySQL               MongoDB
    \                     /
     v                   v
        Redis cache (current lot status)
               |
               v
               Laravel API (GET /lot/{id}/status, GET /sync/health)


## Steps

- ** 1**: `lot_status` MySQL table + Java simulator generating lot
  state changes with occasional bursts. Debezium connector watching the table,
  streaming changes into Kafka topic `mes.mes_db.lot_status`.
- ** 2**: RTD consumer (Java -> MySQL) and ERP consumer (Java -> MongoDB), both
  idempotent on lot_id. Mid-run: add a new column to `lot_status` and show both
  consumers see it automatically.
- ** 3**: Redis cache-aside layer for "current lot status" reads. A deliberately
  slow third consumer simulating a downstream system, to produce a real lag/backpressure
  story during a burst.
- ** 4**: Laravel API (`GET /lot/{id}/status`, `GET /sync/health`). Optional
  Prometheus + Grafana dashboard of the burst.

## Tech stack

| Component          | Tool                     |
| -------------------| -------------------------|
| Source DB          | MySQL                    |
| CDC                | Debezium (Kafka Connect) |
| Event bus          | Kafka                    |
| Simulator          | Java                     |
| RTD-style consumer | Java -> MySQL            |
| ERP-style consumer | Java -> MongoDB          |
| Cache              | Redis                    |
| API                | PHP / Laravel            |
| Orchestration      | Docker Compose           |