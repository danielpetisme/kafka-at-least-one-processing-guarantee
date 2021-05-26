package com.sample.poisonpill;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class GlobalVariables {
	protected final static String CONFIG_COMMON_BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094";
	public final static String CONFIG_COMMON_TOPIC = "poison-pill";

	protected final static String CONFIG_CONSUMER_AUTO_COMMIT_INTERVAL_MS = "1000";
	protected final static String CONFIG_CONSUMER_ENABLE_AUTO_COMMIT = "true";
	protected final static String CONFIG_CONSUMER_GROUP_ID = "test5";
	protected final static String CONFIG_CONSUMER_KEY_DESERIALIZER = StringDeserializer.class.getCanonicalName();
	protected final static String CONFIG_CONSUMER_VALUE_DESERIALIZER = StringDeserializer.class.getCanonicalName();

	protected final static String CONFIG_PRODUCER_ACKS = "all";
	protected final static Integer CONFIG_PRODUCER_BATCH_SIZE = 50000;
	protected final static String CONFIG_PRODUCER_KEY_SERIALIZER = StringSerializer.class.getCanonicalName();
	protected final static Integer CONFIG_PRODUCER_LINGER_MS = 500;
	protected final static Integer CONFIG_PRODUCER_RETRIES = 10;
	protected final static Integer CONFIG_PRODUCER_REQUEST_TIMEOUT_MS = 1000;
	protected final static String CONFIG_PRODUCER_VALUE_SERIALIZER = StringSerializer.class.getCanonicalName();

}
