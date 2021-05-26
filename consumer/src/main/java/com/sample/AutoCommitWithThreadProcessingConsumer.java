package com.sample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AutoCommitWithThreadProcessingConsumer {

    private static final Logger logger = LoggerFactory.getLogger(AutoCommitWithThreadProcessingConsumer.class);

    final long pollIntervalMs;
    final Path path;
    final String topicName;
    final KafkaConsumer<String, String> consumer;
    boolean stopping;

    public static void main(String[] args) throws Exception {
        AutoCommitWithThreadProcessingConsumer simpleConsumer = new AutoCommitWithThreadProcessingConsumer();
        simpleConsumer.start();
    }

    public AutoCommitWithThreadProcessingConsumer() throws ExecutionException, InterruptedException, IOException {
        pollIntervalMs = Long.valueOf(System.getenv().getOrDefault("POLL_INTERVAL_MS", "100"));
        path = Paths.get(System.getenv().getOrDefault("FILE", "/tmp/consumer.out"));
        topicName = System.getenv().getOrDefault("TOPIC", "sample");

        if (Files.exists(path)) {
            Files.delete(path);
        }

        KafkaUtils.createTopic(topicName);

        Properties properties = KafkaUtils.consumerProperties();
        logger.info("Creating consumer with props: {}", properties);
        consumer = new KafkaConsumer<>(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    private void start() {
        try {
            logger.info("Subscribing to {} topic", topicName);
            consumer.subscribe(Collections.singletonList(topicName));

            while (!stopping) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Received record: Key = {}, Value = {}", record.key(), record.value());
                    process(record);
                }

            }
        } finally {
            consumer.close();
        }
    }

    public void process(ConsumerRecord<String, String> record) {
        new Thread(() -> {
            throw new RuntimeException();
        }).start();

    }

    private void stop() {
        logger.info("Stopping the consumer");
        stopping = true;
    }
}
