package com.sample.poisonpill;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.sample.KafkaUtils;

public class SimpleJsonProducer {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		// We create the topic poison-pill, and start adding documents.
		// We are sending events that comprise a String containing JSON.
		// Each several events, we are creating a poison pill, which is a string that is
		// not JSON. If the consumer is deserializing JSON, it will fail. 
		// We are adding sleeps to the code to pace data production; also, in
		// combination with linger.ms and the interval (number of documents) at which we
		// pause, we are ensuring that some batches do not contain poison pills

		int eventsBeforeSleeping = 50;
		int poisonPillEventInterval = eventsBeforeSleeping * 2;
		int totalEvents = eventsBeforeSleeping * 5;
		int lingerMs = 1000;
		int timeToSleepBetweenIntervals = 2 * lingerMs;

		KafkaUtils.createTopic(GlobalVariables.CONFIG_COMMON_TOPIC);
		Properties producerProperties = KafkaClientUtils.getSimpleProducerProperties();
		producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
		Producer<String, String> producer = new KafkaProducer<>(producerProperties);

		try {
			for (int i = 1; i < totalEvents+1; i++) {
				producer.send(

						i % poisonPillEventInterval == 0 ? getWrongJsonRecord(i + "", i)
								: getCorrectJsonRecord(i + "", i),
						(recordMetadata, exception) -> {
							if (exception != null) {
								System.out.println(exception.getMessage());
							}
						});
				if (i % eventsBeforeSleeping == 0) {
					try {
						System.out.println(i + " Sleeping for " + timeToSleepBetweenIntervals + " milliseconds...");
						Thread.sleep(timeToSleepBetweenIntervals);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		} finally {
			producer.close(Duration.ofSeconds(10));
		}
		System.exit(0);
	}

	private static ProducerRecord<String, String> getCorrectJsonRecord(String key, int value) {
		return new ProducerRecord<String, String>(GlobalVariables.CONFIG_COMMON_TOPIC, key,
				"{\"value\":\"" + value + "\"}");
	}

	private static ProducerRecord<String, String> getWrongJsonRecord(String key, int value) {
		return new ProducerRecord<String, String>(GlobalVariables.CONFIG_COMMON_TOPIC, key, "--" + value);
	}
}
