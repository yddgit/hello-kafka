package com.my.project.kafka;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProducerAPITest {

	private Properties config;
	private ProducerAPI producer;

	@Before
	public void setUp() throws Exception {
		config = new Properties();
		config.put("bootstrap.servers", "localhost:9092");
		// ack配置何种情况认为请求已完成
		// all表示所有记录都要确认提交，请求才算完成
		config.put("acks", "all");
		// 请求失败后，producer会自动重试，除非retries设为0
		// 允许重试可能会导致重复提交
		config.put("retries", 0);
		// Producer管理着每个partition未发送数据的buffer，由batch.size进行配置
		// 增大该值可以提高批处理量，但需要更多的内存空间
		// （因为对于每个活动partition通常都会有一个buffer）
		config.put("batch.size", 16384);
		// 默认缓冲区未满也可以发送数据，但如果需要减少请求次数，
		// 可以设置linger.ms参数为大于0的值x。
		// 这样每次请求前如果buffer未满，会等待x毫秒，
		// 使批量提交的数据记录尽可能的多。
		// 当然，如果负载很大，这个参数基本会被忽略，
		// 而设置一个大于0的值可以用微小的延迟换取低负载下更少更有效的请求次数。
		config.put("linger.ms", 1);
		// buffer.memory控制producer可用于缓存数据的总内存大小。
		// 产生数据的速度快于发送数据的速度时缓冲区就会很快被占满，
		// 此时新的请求就会被阻塞，可以用max.block.ms参数指定阻塞的时间，
		// 一旦超时就会抛出TimeoutException
		config.put("buffer.memory", 33554432);
		// key.serializer和value.serializer指定了数据转换的规则
		config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	}

	@After
	public void tearDown() throws Exception {
		producer.close();
	}

	@Test
	public void testProduce() {
		producer = new ProducerAPI(config);
		Map<String, String> record = new LinkedHashMap<String, String>();
		record.put("key1", "value1");
		record.put("key2", "value2");
		record.put("key3", "value3");
		record.put("key4", "value4");
		record.put("key5", "value5");
		producer.produce("my-topic", record);
	}

	@Test
	public void testProduceWithTransaction() {
		String transactionId = "t1";
		config.put("retries", Integer.MAX_VALUE);
		config.put("transactional.id", transactionId);
		producer = new ProducerAPI(config);
		Map<String, String> record = new LinkedHashMap<String, String>();
		record.put(transactionId + ":key1", "value1");
		record.put(transactionId + ":key2", "value2");
		record.put(transactionId + ":key3", "value3");
		record.put(transactionId + ":key4", "value4");
		record.put(transactionId + ":key5", "value5");
		producer.produceWithTransaction("my-topic", record);
	}
}
