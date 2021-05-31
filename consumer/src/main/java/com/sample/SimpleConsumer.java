package com.sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleConsumer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

    public static final int ONE_MB = 1024 * 1024;
    public static final int ONE_SECOND = 1000;
    public static final int POLL_TIMEOUT_SECONDS = 15;

    private static final Map<String, String> DEFAULT_CONFIG = Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:19093,localhost:19094",
        ConsumerConfig.GROUP_ID_CONFIG, "simple-consumer",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
    );

    final Path path;
    final String topicName;
    final KafkaConsumer<String, String> consumer;
    boolean stopping;

    public static void main(String[] args) throws Exception {
        SimpleConsumer simpleConsumer = new SimpleConsumer();
        simpleConsumer.start();
    }

    public SimpleConsumer() throws ExecutionException, InterruptedException, IOException {
        path = Paths.get(System.getenv().getOrDefault("FILE", "/tmp/consumer.out"));
        topicName = System.getenv().getOrDefault("TOPIC", "sample");

        Properties properties = new Properties();
        properties.putAll(DEFAULT_CONFIG);
        properties.putAll(KafkaUtils.systemProperties());
        //for test purpose we increase the below limits to create a batch of records
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1 * ONE_MB); //default 1B
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 10 * ONE_SECOND); //default 500ms
        // for test purpose we lower the limit to generate consumer group timeouts
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 20 * ONE_SECOND); //default 5m

        KafkaUtils.createTopic(properties, topicName);

        logger.info("Creating consumer with props: {}", properties);
        consumer = new KafkaConsumer<>(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    private void start() {
        try {
            logger.info("Subscribing to {} topic", topicName);
            consumer.subscribe(Collections.singletonList(topicName), KafkaUtils.rebalanceListener);

            while (!stopping) {
                pollAndProcess();
            }
        } finally {
            // What can happen here?
//            consumer.commitSync();
            consumer.close();
        }
    }

    private void pollAndProcess() {
        long pollStart = System.currentTimeMillis();
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(POLL_TIMEOUT_SECONDS));
        if (records.isEmpty()) {
            return;
        }
        logger.info("Received {} records starting at offset: {}", records.count(), consumer.position(new TopicPartition(topicName, 0)));

        for (ConsumerRecord<String, String> record : records) {
            logger.info("Received record: Key = {}, Value = {}", record.key(), record.value());
            processRemoteSystem(record, 5 * 1000);
//             processRemoteSystemTimeout(record);
//            processRemoteSystemException(record);

//                    // Commit after a record
//                    // After each record
//                    // No offsets passed
//                    // 1- Sync commit
//                    consumer.commitSync();
//                    // 2- Async commit
//                    consumer.commitAsync((offsets, exception) -> {
//                        logger.info("Committed offsets = {}, ex = {}", offsets, exception);
//                    });
//
//                    // Offset passed, remember how Kafka is persisting offset progression :-)
//                    // 1- Sync commit
//                    consumer.commitSync(Map.of(
//                        new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata().
//                    ), Duration.ofMillis(100));
//                    // 2- Async commit
//                    consumer.commitAsync(Map.of(
//                        new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata()
//                    ), (offsets, exception) -> {
//                        logger.info("Committed offsets = {}, exception = {}", offsets, exception);
//                    });

        }
//                    // Commit after a batch processing
        long pollEnd = System.currentTimeMillis();
        logger.info("Polled and processed {} records in {} ms", records.count(), pollEnd - pollStart);

    }


    public void processRemoteSystem(ConsumerRecord<String, String> record, long timeout) {
        List<String> lines = Collections.singletonList(record.value());

        try {
            for (String l : lines) {
                logger.info("Processing line: {}, taking {} ms", l, timeout);
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
            for (String l : lines) {
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
            for (String l : lines) {
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
