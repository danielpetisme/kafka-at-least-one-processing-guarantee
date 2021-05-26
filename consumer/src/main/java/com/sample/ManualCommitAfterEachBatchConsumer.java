package com.sample;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ManualCommitAfterEachBatchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ManualCommitAfterEachBatchConsumer.class);

    final long pollIntervalMs;
    final Path path;
    final String topicName;
    final KafkaConsumer<String, String> consumer;
    final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap();
    boolean stopping;

    public static void main(String[] args) throws Exception {
        ManualCommitAfterEachBatchConsumer simpleConsumer = new ManualCommitAfterEachBatchConsumer();
        simpleConsumer.start();
    }

    public ManualCommitAfterEachBatchConsumer() throws ExecutionException, InterruptedException, IOException {
        pollIntervalMs = Long.valueOf(System.getenv().getOrDefault("POLL_INTERVAL_MS", "100"));
        path = Paths.get(System.getenv().getOrDefault("FILE", "/tmp/consumer.out"));
        topicName = System.getenv().getOrDefault("TOPIC", "sample");

        if (Files.exists(path)) {
            Files.delete(path);
        }

        KafkaUtils.createTopic(topicName);

        Properties properties = KafkaUtils.consumerProperties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
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
                    try {
                        process(record);
                        offsetsToCommit.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                        logger.info("Processed offsets: {}", offsetsToCommit);
                    } catch (MyBusinessException e) {
                        logger.warn("Business exception occurred on offset = {}", record.offset());
                        break;
                    }
                }
//
//                    // Offset passed
//                    // 1- Sync commit
//                    consumer.commitSync(Map.of(
//                        new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()).
//                    ), Duration.ofMillis(100));
//                    // 2- Async commit
                if (!records.isEmpty()) {
                    logger.info("Committing offsets = {}", offsetsToCommit);
                    consumer.commitAsync(offsetsToCommit, (offsets, exception) -> {
                        logger.info("Committed offsets = {}, exception = {}", offsets, exception);
                    });
                }

            }
        } finally {
            consumer.commitSync(offsetsToCommit, Duration.ofMillis(100));
            consumer.close();

        }
    }

    public void process(ConsumerRecord<String, String> record) throws MyBusinessException {
        long option = System.currentTimeMillis() % 3;
        if (option == 0) {
            try {
                logger.info("Sleeping 2s...");
                Thread.sleep(2 * 1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (option == 1) {
            logger.info("Generating an exception...");
            throw new MyBusinessException();
        }
        logger.info("Processed record: Key = {}, Value = {}", record.key(), record.value());
    }

    private void stop() {
        logger.info("Stopping the consumer");
        stopping = true;
    }

    public static class MyBusinessException extends Throwable {

    }
}
