# Day 1 — CDC pipeline proof

Captured from `kafka-console-consumer` on the `mes.mes_db.lot_status` topic,
while the Java simulator was running.

This message shows lot `LOT-00145` moving from `started` on `TOOL-14` to
`in_process` on `TOOL-07`. Debezium captured both the before and after row
state automatically from MySQL's binlog, with no polling and no manual diff.

```json
{
  "before": {
    "lot_id": "LOT-00145",
    "state": "started",
    "tool_id": "TOOL-14",
    "updated_at": "2026-06-28T06:49:18Z"
  },
  "after": {
    "lot_id": "LOT-00145",
    "state": "in_process",
    "tool_id": "TOOL-07",
    "updated_at": "2026-06-28T06:49:32Z"
  },
  "op": "u",
  "source": {
    "file": "mysql-bin.000003",
    "pos": 197913,
    "connector": "mysql",
    "db": "mes_db",
    "table": "lot_status"
  }
}
```

`op: "u"` means update. The `source.file` and `source.pos` fields are the
exact binlog filename and byte offset Debezium read this change from —
confirming the event came from the real MySQL transaction log, not a poll.

