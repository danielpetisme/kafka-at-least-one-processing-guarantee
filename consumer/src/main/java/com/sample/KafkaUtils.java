package com.sample;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class KafkaUtils {

    private static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);

    private static final String DEFAULT_KAFKA_ENV_PREFIX = "KAFKA_";

    public static final String NUMBER_OF_PARTITIONS = "NUMBER_OF_PARTITIONS";
    public static final String REPLICATION_FACTOR = "REPLICATION_FACTOR";
    public static final String DEFAULT_NUMBER_OF_PARTITIONS = "1";
    public static final String DEFAULT_REPLICATION_FACTOR = "3";

    public static Map<String, String> systemProperties() {
        return systemProperties(System.getenv(), DEFAULT_KAFKA_ENV_PREFIX);
    }

    public static Map<String, String> systemProperties(Map<String, String> envProps, String prefix) {
        return envProps.entrySet()
            .stream()
            .filter(e -> e.getKey().startsWith(prefix))
            .collect(Collectors.toMap(
                e -> e.getKey()
                    .replace(prefix, "")
                    .toLowerCase()
                    .replace("_", ".")
                , e -> e.getValue())
            );
    }


    public static void createTopic(Properties properties, String topicName) throws ExecutionException, InterruptedException {
        final Integer numberOfPartitions = Integer.valueOf(System.getenv().getOrDefault(NUMBER_OF_PARTITIONS, DEFAULT_NUMBER_OF_PARTITIONS));
        final Short replicationFactor = Short.valueOf(System.getenv().getOrDefault(REPLICATION_FACTOR, DEFAULT_REPLICATION_FACTOR));

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

    public static final ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.printf("onPartitionsRevoked - %s%n", formatPartitions(partitions));
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            System.out.printf("onPartitionsLost - %s%n", formatPartitions(partitions));
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.printf("onPartitionsAssigned - %s%n", formatPartitions(partitions));
        }
    };

    private static List<String> formatPartitions(Collection<TopicPartition> partitions) {
        return partitions.stream().map(topicPartition ->
            String.format("topic: %s, partition: %s", topicPartition.topic(), topicPartition.partition()))
            .collect(Collectors.toList());
    }

}
