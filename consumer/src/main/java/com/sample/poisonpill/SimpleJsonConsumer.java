package com.sample.poisonpill;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;

public class SimpleJsonConsumer {
	
	public static void main(String[] args) throws InterruptedException {
		// A consumer that blocks on a poison pill
//		playBlockingConsumer();
		// A consumer that skips batches in a poison pill
		playBatchSkippingConsumer();
		// A consumer that skips poison pills
//		playRecordSkippingConsumer();
	}
	
	private static void playBlockingConsumer() throws InterruptedException {
		// This consumer does not handle deserialization errors; instead, it will keep trying.
		Properties jsonConsumerProperties = KafkaClientUtils.getSimpleConsumerProperties();
		jsonConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				KafkaJsonDeserializer.class.getCanonicalName());
		jsonConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		jsonConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(jsonConsumerProperties);
		consumer.subscribe(Arrays.asList(new String[] { GlobalVariables.CONFIG_COMMON_TOPIC }));

		try {
			while (true) {
				try {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
					System.out.println("polled " + records.count() + " records");
					for (ConsumerRecord<String, String> record : records) {
						System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
								record.value());
					}
					consumer.commitSync();
				} catch (SerializationException se) {
					Thread.sleep(1000);
					System.out.println(se.getMessage());
				}
			}
		} finally {
			consumer.close(Duration.ofSeconds(30));
		}
	}

	private static void playBatchSkippingConsumer() throws InterruptedException {
		Properties jsonConsumerProperties = KafkaClientUtils.getSimpleConsumerProperties();
		jsonConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				KafkaJsonDeserializer.class.getCanonicalName());
		jsonConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		jsonConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//		jsonConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(jsonConsumerProperties);
		consumer.subscribe(Arrays.asList(new String[] { GlobalVariables.CONFIG_COMMON_TOPIC }));

		try {
			Set<TopicPartition> partitions = null;
			while (true) {
				try {
					try {
					System.out.println(consumer.committed(new TopicPartition("poison-pill",0)));
					System.out.println(consumer.position(new TopicPartition("poison-pill",0)));
					// consumer.seek(new TopicPartition("poison-pill",0), consumer.committed(new TopicPartition("poison-pill",0)));
					} catch (Exception e) {}
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
					System.out.println("polled " + records.count() + " records");
					partitions = records.partitions();
					for (ConsumerRecord<String, String> record : records) {
						System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
								record.value());
					}
					consumer.commitSync();
				} catch (SerializationException se) {
					// How to get partition and failed offset?? This is a Jackson failure, we do not have info other than the text message...
					// This is a very weak way of handling the error. 
					Thread.sleep(1000);
					String message = se.getMessage();
					message = message.replace("Error deserializing key/value for partition ", "").replace(". If needed, please seek past the record to continue consumption.", "").replace(" at offset ", " ");
					String[] split = message.split(" ");
					int offset = Integer.parseInt(split[1]);
					int indexDash = split[0].lastIndexOf("-");
					String topic = split[0].substring(0,indexDash);
					int partition = Integer.parseInt(split[0].substring(indexDash+1));
					System.out.println(se.getMessage());
					System.out.println("Committing to bypass the poison pill");
					Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap();
					toCommit.put(new TopicPartition(topic,partition),new OffsetAndMetadata(offset+2));
					consumer.commitSync(toCommit);
				}
			}
		} finally {
			consumer.close(Duration.ofSeconds(30));
		}
	}

	private static void playRecordSkippingConsumer() throws InterruptedException {
		// This consumer deserializes bytes first, and then, event by event, JSON, so we can figure out easily which event failed and continue processing. 
		Properties jsonConsumerProperties = KafkaClientUtils.getSimpleConsumerProperties();
		jsonConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				ByteArrayDeserializer.class.getCanonicalName());
		jsonConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(jsonConsumerProperties);
		consumer.subscribe(Arrays.asList(new String[] { GlobalVariables.CONFIG_COMMON_TOPIC }));
		KafkaJsonDeserializer<String> deserializer = new KafkaJsonDeserializer<>();
		Map configuration = new HashMap();
		configuration.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, "java.lang.Object");
		deserializer.configure(configuration, false);

		try {
			while (true) {
				try {
					ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
					System.out.println("polled " + records.count() + " records");
					for (ConsumerRecord<String, byte[]> record : records) {
						try {
							Object value = deserializer.deserialize("", record.value());

							System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
									value);
						} catch (SerializationException seInternal) {
							System.out.println("Skipping record " + record.key());
						}
					}
					consumer.commitSync();
				} catch (SerializationException se) {
					Thread.sleep(1000);
					System.out.println(se.getMessage());
				}
			}
		} finally {
			consumer.close(Duration.ofSeconds(30));
			deserializer.close();
		}
	}


}
