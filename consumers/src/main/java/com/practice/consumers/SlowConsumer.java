package com.practice.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

/**
 * A deliberately slow consumer simulating a downstream system that cannot
 * keep up with burst traffic — for example a reporting pipeline or audit
 * logger with limited throughput.
 *
 * Real-world parallel: in the fab, downstream systems like yield analysis
 * or ERP reporting consumed lot events at their own pace, independently
 * of how fast equipment events were arriving. During end-of-batch cascades,
 * those systems fell behind — Kafka holds the backlog durably so nothing
 * is lost, and the system catches up after the burst subsides.
 *
 * This consumer deliberately sleeps 500ms per message to make lag
 * visible and measurable on the Grafana dashboard.
 */
public class SlowConsumer {

    private static final String TOPIC = "mes.mes_db.lot_status";
    private static final String GROUP_ID = "slow-consumer-group";
    private static final int PROCESSING_DELAY_MS = 500;

    private final KafkaConsumer<String, String> consumer;
    private final DebeziumEventParser parser = new DebeziumEventParser();

    public SlowConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // latest = only process new messages from this point forward,
        // don't try to catch up on the entire backlog from earlier step 1 and 2
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        this.consumer = new KafkaConsumer<>(props);
    }

    public void run() {
        consumer.subscribe(Collections.singletonList(TOPIC));
        System.out.println("[Slow consumer] subscribed to " + TOPIC + ", processing at " +
                           PROCESSING_DELAY_MS + "ms per message...");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    DebeziumEventParser.ParsedEvent event = parser.parse(record.value());
                    if (event.isDelete()) continue;

                    String lotId = event.after.get("lot_id");
                    String state = event.after.get("state");

                    // Calculate how far behind we are by comparing message
                    // timestamp (when it was produced) to now
                    long producedAt = record.timestamp();
                    long lagMs = Instant.now().toEpochMilli() - producedAt;

                    System.out.println("[Slow consumer] processing lot=" + lotId +
                                       " state=" + state +
                                       " lag=" + lagMs + "ms");

                    // Simulate slow processing work
                    Thread.sleep(PROCESSING_DELAY_MS);

                } catch (Exception e) {
                    System.err.println("[Slow consumer] error: " + e.getMessage());
                }
            }
        }
    }

    public static void main(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        new SlowConsumer(bootstrapServers).run();
    }
}

