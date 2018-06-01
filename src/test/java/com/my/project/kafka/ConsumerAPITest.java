package com.my.project.kafka;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConsumerAPITest {

	private Properties config;
	private ConsumerAPI consumer;

	@Before
	public void setUp() throws Exception {
		config = new Properties();
		config.put("bootstrap.servers", "localhost:9092");
		config.put("group.id", "test");
		config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	}

	@After
	public void tearDown() throws Exception {
		consumer.close();
	}

	@Test
	public void testAutoOffsetCommit() {
		// offset会以auto.commit.interval.ms指定的周期自动提交
		config.put("enable.auto.commit", "true");
		config.put("auto.commit.interval.ms", "1000");
		consumer = new ConsumerAPI(config);
		consumer.consumeWithAutoOffsetCommit("my-topic", "test");
	}

	@Test
	public void testManualOffsetCommit() {
		config.put("enable.auto.commit", "false");
		consumer = new ConsumerAPI(config);
		consumer.consumeWithManualOffsetCommit("my-topic");
	}
	
	@Test
	public void testPartitionManualOffsetCommit() {
		config.put("enable.auto.commit", "false");
		consumer = new ConsumerAPI(config);
		consumer.consumePartitionWithManualOffsetCommit("my-topic", "test");
	}
}
