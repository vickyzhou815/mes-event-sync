package com.practice.consumers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Shared helper for both consumers: extracts the meaningful fields out of a
 * raw Debezium change event, ignoring the verbose schema boilerplate.
 *
 * Debezium messages look like: {"schema": {...lots of type info...}, "payload": {"before": {...}, "after": {...}, "op": "u"}}
 * We only care about payload.after (the row's new state) and payload.op
 * (c=create, u=update, d=delete).
 */
public class DebeziumEventParser {

    private final ObjectMapper mapper = new ObjectMapper();

    public static class ParsedEvent {
        public final String operation;       // "c", "u", or "d"
        public final Map<String, String> after; // field name -> value, null if this was a delete

        public ParsedEvent(String operation, Map<String, String> after) {
            this.operation = operation;
            this.after = after;
        }

        public boolean isDelete() {
            return "d".equals(operation);
        }
    }

    public ParsedEvent parse(String rawJson) throws Exception {
        JsonNode root = mapper.readTree(rawJson);
        JsonNode payload = root.get("payload");

        String op = payload.get("op").asText();
        JsonNode afterNode = payload.get("after");

        Map<String, String> after = new HashMap<>();
        if (afterNode != null && !afterNode.isNull()) {
            afterNode.fields().forEachRemaining(entry ->
                after.put(entry.getKey(), entry.getValue().asText())
            );
        }

        return new ParsedEvent(op, after);
    }
}

