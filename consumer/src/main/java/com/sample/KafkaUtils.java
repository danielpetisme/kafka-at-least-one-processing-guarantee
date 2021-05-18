package com.sample;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class KafkaUtils {

    private static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);

    private static final String DEFAULT_KAFKA_ENV_PREFIX = "KAFKA_";

    public static final String NUMBER_OF_PARTITIONS = "NUMBER_OF_PARTITIONS";
    public static final String REPLICATION_FACTOR = "REPLICATION_FACTOR";
    public static final String DEFAULT_NUMBER_OF_PARTITIONS = "1";
    public static final String DEFAULT_REPLICATION_FACTOR = "3";

    private static final Map<String, String> DEFAULT_CONFIG = Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:19093,localhost:19094",
        ConsumerConfig.GROUP_ID_CONFIG, "simple-consumer",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    public static final Properties consumerProperties() {
        return consumerProperties(DEFAULT_CONFIG, System.getenv(), DEFAULT_KAFKA_ENV_PREFIX);
    }

    private static Properties consumerProperties(Map<String, String> baseProps, Map<String, String> envProps, String prefix) {
        Map<String, String> systemProperties = envProps.entrySet()
            .stream()
            .filter(e -> e.getKey().startsWith(prefix))
            .collect(Collectors.toMap(
                e -> e.getKey()
                    .replace(prefix, "")
                    .toLowerCase()
                    .replace("_", ".")
                , e -> e.getValue())
            );

        Properties props = new Properties();
        props.putAll(baseProps);
        props.putAll(systemProperties);
        return props;
    }

    public static void createTopic(String topicName) throws ExecutionException, InterruptedException {
        Properties properties = consumerProperties();
        final Integer numberOfPartitions =  Integer.valueOf(System.getenv().getOrDefault(NUMBER_OF_PARTITIONS, DEFAULT_NUMBER_OF_PARTITIONS));
        final Short replicationFactor =  Short.valueOf(System.getenv().getOrDefault(REPLICATION_FACTOR, DEFAULT_REPLICATION_FACTOR));

        AdminClient adminClient = KafkaAdminClient.create(properties);
        createTopic(adminClient, topicName, numberOfPartitions, replicationFactor);
    }

    private static void createTopic(AdminClient adminClient, String topicName, int partitionCount, int replicationFactor) throws ExecutionException, InterruptedException {
        if (!adminClient.listTopics().names().get().contains(topicName)) {
            logger.info("Creating topic {}", topicName);
            final NewTopic newTopic = new NewTopic(topicName, partitionCount, (short) replicationFactor);
            try {
                CreateTopicsResult topicsCreationResult = adminClient.createTopics(Collections.singleton(newTopic));
                topicsCreationResult.all().get();
            } catch (ExecutionException e) {
                //silent ignore if topic already exists
            }
        }

    }
}
