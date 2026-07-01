package com.practice.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * RTD consumer — updated to also write current lot status
 * into Redis after each MySQL upsert. This is the cache-aside pattern:
 * Redis always reflects the latest state, so status reads can hit Redis
 * first instead of hammering MySQL directly.
 */
public class RtdConsumer {

    private static final String TOPIC = "mes.mes_db.lot_status";
    private static final String GROUP_ID = "rtd-consumer-group";
    private static final int REDIS_TTL_SECONDS = 3600; // cache entries expire after 1 hour

    private final KafkaConsumer<String, String> consumer;
    private final DebeziumEventParser parser = new DebeziumEventParser();
    private final String jdbcUrl;
    private final String dbUser;
    private final String dbPassword;
    private final Jedis jedis;

    public RtdConsumer(String bootstrapServers, String jdbcUrl, String dbUser,
                       String dbPassword, String redisHost, int redisPort) {
        this.jdbcUrl = jdbcUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.jedis = new Jedis(redisHost, redisPort);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
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
                        upsertMySQL(conn, event.after);
                        writeToRedis(event.after);
                    } catch (Exception e) {
                        System.err.println("[RTD consumer] failed to process record: " + e.getMessage());
                    }
                }
            }
        }
    }

    private void upsertMySQL(Connection conn, Map<String, String> after) throws SQLException {
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
            System.out.println("[RTD consumer] MySQL upserted lot=" + lotId + " state=" + state);
        }
    }

    private void writeToRedis(Map<String, String> after) {
        String lotId = after.get("lot_id");
        String state = after.get("state");
        String toolId = after.get("tool_id");

        // Key pattern: "lot:{lot_id}" -> "state={state},tool={toolId}"
        // TTL means stale entries self-heal even if a Redis write was missed
        String key = "lot:" + lotId;
        String value = "state=" + state + ",tool=" + toolId;
        jedis.setex(key, REDIS_TTL_SECONDS, value);
        System.out.println("[RTD consumer] Redis cached lot=" + lotId + " -> " + value);
    }

    public static void main(String[] args) throws SQLException {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String jdbcUrl          = args.length > 1 ? args[1] : "jdbc:mysql://localhost:3306/mes_db";
        String dbUser           = args.length > 2 ? args[2] : "root";
        String dbPassword       = args.length > 3 ? args[3] : "practice";
        String redisHost        = args.length > 4 ? args[4] : "localhost";
        int    redisPort        = args.length > 5 ? Integer.parseInt(args[5]) : 6379;
        new RtdConsumer(bootstrapServers, jdbcUrl, dbUser, dbPassword, redisHost, redisPort).run();
    }
}