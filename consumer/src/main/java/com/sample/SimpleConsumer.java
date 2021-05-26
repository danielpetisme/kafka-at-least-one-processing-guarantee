package com.sample;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleConsumer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

    final int pollIntervalMs;
    final int pollRecords;
    final Path path;
    final String topicName;
    final KafkaConsumer<String, String> consumer;
    boolean stopping;

    public static void main(String[] args) throws Exception {
        SimpleConsumer simpleConsumer = new SimpleConsumer();
        simpleConsumer.start();
    }

    public SimpleConsumer() throws ExecutionException, InterruptedException, IOException {
        // https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#consumerconfigs_max.poll.interval.ms
        pollIntervalMs = Integer.parseInt(System.getenv().getOrDefault("POLL_INTERVAL_MS", "100"));
        pollRecords = Integer.parseInt(System.getenv().getOrDefault("POLL_RECORDS", "10"));

        path = Paths.get(System.getenv().getOrDefault("FILE", "/tmp/consumer.out"));
        topicName = System.getenv().getOrDefault("TOPIC", "sample");

        if (Files.exists(path)) {
            Files.delete(path);
        }

        KafkaUtils.createTopic(topicName);

        Properties properties = KafkaUtils.consumerProperties();
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, pollIntervalMs);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, pollRecords);
        logger.info("Creating consumer with props: {}", properties);
        consumer = new KafkaConsumer<>(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    private void start() {
        try {
            logger.info("Subscribing to {} topic", topicName);
            consumer.subscribe(Collections.singletonList(topicName), KafkaUtils.rebalanceListener);

            while (!stopping) {
                long pollStart = System.currentTimeMillis();
                pollAndProcess();
                long pollEnd = System.currentTimeMillis();
                logger.info("poll and process loop took {} ms", pollEnd - pollStart);
            }
        } finally {
            // What can happen here?
//            consumer.commitSync(Duration.ofMillis(100));
            consumer.close();
        }
    }

    private void pollAndProcess() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
            logger.info("Received record: Key = {}, Value = {}", record.key(), record.value());
//             processRemoteSystem(record, 5 * 1000);
//             processRemoteSystemTimeout(record);
//            processRemoteSystemException(record);

//                    // Commit after a record
//                    // After each record
//                    // No offsets passed == commit the polled batch
//                    // 1- Sync commit - Useless, commit each batch records.length() times, increase latency and broker workload
//                    consumer.commitSync(Duration.ofMillis(100));
//                    // 2- Async commit - Useless for the same reasons but since its async lower impact on latency/throughput
//                    consumer.commitAsync((offsets, exception) -> {
//                        logger.info("Committed offsets = {}, ex = {}", offsets.get(new TopicPartition(record.topic(), record.partition())).offset(), exception);
//                    });
//
//                    // Offset passed Remember your need to commit offset +1
//                    // 1- Sync commit
//                    consumer.commitSync(Map.of(
//                        new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()).
//                    ), Duration.ofMillis(100));
//                    // 2- Async commit
//                    consumer.commitAsync(Map.of(
//                        new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1)
//                    ), (offsets, exception) -> {
//                        logger.info("Committed offsets = {}, exception = {}", offsets.get(new TopicPartition(record.topic(), record.partition())).offset() + 1, exception);
//                    });

        }
//                    // Commit after a batch processing
//                    // Offset passed
//                    // 1- Sync commit
//                    consumer.commitSync(Map.of(
//                        new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()).
//                    ), Duration.ofMillis(100));
//                    // 2- Async commit
//                if (!records.isEmpty()) {
//                    logger.info("Committing offsets = {}", offsetsToCommit);
//                    consumer.commitAsync(offsetsToCommit, (offsets, exception) -> {
//                        logger.info("Committed offsets = {}, exception = {}", offsets, exception);
//                    });
    }


    public void processRemoteSystem(ConsumerRecord<String, String> record, long timeout) {
        List<String> lines = Collections.singletonList(record.value());

        try {
            for(String l : lines) {
                logger.info("processing line: {}, taking {} ms", l, timeout);
                Files.write(path, Collections.singletonList(l), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                Thread.sleep(timeout);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void processRemoteSystemTimeout(ConsumerRecord<String, String> record) {
        List<String> lines = Collections.singletonList(record.value());

        try {
            for(String l : lines) {
                logger.info("processing line: {}, blocking endlessly", l);
                Files.write(path, Collections.singletonList(l), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                Thread.sleep(Long.MAX_VALUE);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void processRemoteSystemException(ConsumerRecord<String, String> record) {
        List<String> lines = Collections.singletonList(record.value());

        try {
            for(String l : lines) {
                logger.info("processing line: {}, failing", l);
                Files.write(path, Collections.singletonList(l), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                throw new RuntimeException("remote system kaboom!");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void stop() {
        logger.info("Stopping the consumer");
        stopping = true;
    }

}
