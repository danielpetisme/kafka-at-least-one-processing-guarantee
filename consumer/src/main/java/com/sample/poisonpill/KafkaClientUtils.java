package com.sample.poisonpill;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;



public class KafkaClientUtils {

	public static Properties getSimpleConsumerProperties() {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				GlobalVariables.CONFIG_COMMON_BOOTSTRAP_SERVERS);
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GlobalVariables.CONFIG_CONSUMER_GROUP_ID);
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
				GlobalVariables.CONFIG_CONSUMER_ENABLE_AUTO_COMMIT);
		properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
				GlobalVariables.CONFIG_CONSUMER_AUTO_COMMIT_INTERVAL_MS);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				GlobalVariables.CONFIG_CONSUMER_KEY_DESERIALIZER);
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				GlobalVariables.CONFIG_CONSUMER_KEY_DESERIALIZER);
		return properties;
	}

	
	public static Properties getSimpleProducerProperties() {
		return getSimpleProducerProperties("SimpleProducer");
	} 
	
	public static Properties getSimpleProducerProperties(String clientId) {
		Properties properties = new Properties();
		properties.put(ProducerConfig.ACKS_CONFIG, GlobalVariables.CONFIG_PRODUCER_ACKS);
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, GlobalVariables.CONFIG_PRODUCER_BATCH_SIZE);
		properties.put(ProducerConfig.LINGER_MS_CONFIG, GlobalVariables.CONFIG_PRODUCER_LINGER_MS);
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalVariables.CONFIG_COMMON_BOOTSTRAP_SERVERS);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, GlobalVariables.CONFIG_PRODUCER_KEY_SERIALIZER);
		properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, GlobalVariables.CONFIG_PRODUCER_REQUEST_TIMEOUT_MS);
		properties.put(ProducerConfig.RETRIES_CONFIG, GlobalVariables.CONFIG_PRODUCER_RETRIES);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlobalVariables.CONFIG_PRODUCER_VALUE_SERIALIZER);
		return properties;
	}
	
}
