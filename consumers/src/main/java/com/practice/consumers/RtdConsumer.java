package com.practice.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Simulates the "RTD" side of the fan-out: an independent consumer reading
 * the same Kafka topic as the ERP consumer, writing normalized rows to its
 * own MySQL table. Idempotent on lot_id via upsert, so a duplicate delivery
 * from Kafka never creates a duplicate or corrupt row.
 */
public class RtdConsumer {

    private static final String TOPIC = "mes.mes_db.lot_status";
    private static final String GROUP_ID = "rtd-consumer-group";

    private final KafkaConsumer<String, String> consumer;
    private final DebeziumEventParser parser = new DebeziumEventParser();
    private final String jdbcUrl;
    private final String dbUser;
    private final String dbPassword;

    public RtdConsumer(String bootstrapServers, String jdbcUrl, String dbUser, String dbPassword) {
        this.jdbcUrl = jdbcUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // earliest = if this consumer group has never read before, start from
        // the beginning of the topic rather than only new messages
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<>(props);
    }

    public void run() throws SQLException {
        consumer.subscribe(Collections.singletonList(TOPIC));
        System.out.println("[RTD consumer] subscribed to " + TOPIC + ", waiting for events...");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)) {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        DebeziumEventParser.ParsedEvent event = parser.parse(record.value());
                        if (event.isDelete()) {
                            System.out.println("[RTD consumer] skipping delete event");
                            continue;
                        }
                        upsert(conn, event.after);
                    } catch (Exception e) {
                        System.err.println("[RTD consumer] failed to process record: " + e.getMessage());
                    }
                }
            }
        }
    }

    private void upsert(Connection conn, Map<String, String> after) throws SQLException {
        String lotId = after.get("lot_id");
        String state = after.get("state");
        String toolId = after.get("tool_id");

        String sql = "INSERT INTO lot_status_rtd (lot_id, state, tool_id) VALUES (?, ?, ?) " +
                      "ON DUPLICATE KEY UPDATE state = ?, tool_id = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, lotId);
            stmt.setString(2, state);
            stmt.setString(3, toolId);
            stmt.setString(4, state);
            stmt.setString(5, toolId);
            stmt.executeUpdate();
            System.out.println("[RTD consumer] upserted lot=" + lotId + " state=" + state);
        }
    }

    public static void main(String[] args) throws SQLException {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String jdbcUrl = args.length > 1 ? args[1] : "jdbc:mysql://localhost:3306/mes_db";
        String dbUser = args.length > 2 ? args[2] : "root";
        String dbPassword = args.length > 3 ? args[3] : "practice";
        new RtdConsumer(bootstrapServers, jdbcUrl, dbUser, dbPassword).run();
    }
}

