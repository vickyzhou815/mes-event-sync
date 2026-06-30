package com.practice.consumers;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Simulates the "ERP" side of the fan-out: an independent consumer reading
 * the same Kafka topic as the RTD consumer, writing normalized documents to
 * MongoDB. Idempotent on lot_id via replaceOne+upsert, the Mongo equivalent
 * of the MySQL "ON DUPLICATE KEY UPDATE" pattern.
 */
public class ErpConsumer {

    private static final String TOPIC = "mes.mes_db.lot_status";
    private static final String GROUP_ID = "erp-consumer-group";

    private final KafkaConsumer<String, String> consumer;
    private final DebeziumEventParser parser = new DebeziumEventParser();
    private final MongoCollection<Document> collection;

    public ErpConsumer(String bootstrapServers, String mongoUri) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<>(props);

        MongoClient mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase("erp_db");
        this.collection = database.getCollection("lot_status_erp");
    }

    public void run() {
        consumer.subscribe(Collections.singletonList(TOPIC));
        System.out.println("[ERP consumer] subscribed to " + TOPIC + ", waiting for events...");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    DebeziumEventParser.ParsedEvent event = parser.parse(record.value());
                    if (event.isDelete()) {
                        System.out.println("[ERP consumer] skipping delete event");
                        continue;
                    }
                    upsert(event.after);
                } catch (Exception e) {
                    System.err.println("[ERP consumer] failed to process record: " + e.getMessage());
                }
            }
        }
    }

    private void upsert(Map<String, String> after) {
        String lotId = after.get("lot_id");

        Document filter = new Document("lot_id", lotId);
        Document doc = new Document("lot_id", lotId)
                .append("state", after.get("state"))
                .append("tool_id", after.get("tool_id"))
                .append("source_updated_at", after.get("updated_at"));

        collection.replaceOne(filter, doc, new ReplaceOptions().upsert(true));
        System.out.println("[ERP consumer] upserted lot=" + lotId + " state=" + after.get("state"));
    }

    public static void main(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String mongoUri = args.length > 1 ? args[1] : "mongodb://localhost:27017";
        new ErpConsumer(bootstrapServers, mongoUri).run();
    }
}

