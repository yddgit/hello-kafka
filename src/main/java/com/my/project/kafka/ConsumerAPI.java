package com.my.project.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class ConsumerAPI implements AutoCloseable {

	private Consumer<String, String> consumer;

	public ConsumerAPI(Properties config) {
		this.consumer = new KafkaConsumer<>(config);
	}

	/**
	 * Automatic Offset Committing
	 * @param topic
	 */
	public void consumeWithAutoOffsetCommit(String... topic) {
		this.consumer.subscribe(Arrays.asList(topic));
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for(ConsumerRecord<String, String> record : records) {
				System.out.printf("topic = %s, offset = %d, key = %s, value = %s%n", record.topic(), record.offset(), record.key(), record.value());
			}
		}
	}

	/**
	 * Manual Offset Control
	 * @param topic
	 */
	public void consumeWithManualOffsetCommit(String... topic) {
		this.consumer.subscribe(Arrays.asList(topic));
		final int minBatchSize = 25;
		List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for(ConsumerRecord<String, String> record : records) {
				buffer.add(record);
			}
			System.out.println("buffer.size=" + buffer.size());
			if(buffer.size() >= minBatchSize) {
				// 将消费的数据缓存在内存中，当缓存的数据足够多的时候，再将其插入数据库中。
				// 如果允许offset自动提交，那当poll()接收到消息时就认为消费完成了，
				// 但在数据插入数据库之前，程序可能会出错，这就会导致数据丢失。
				
				// 为了避免上述问题，需要在对应的数据记录成功插入数据库之后再提交offset。
				// 这使得我们能够明确的控制消息何时消费完成。但是，在数据库提交成功之后，
				// 手动提交offset之前，程序可能出错。这就会导致数据重复，
				// 这就是Kafka的at-least-once的消息投递保证：通常只会投递一次，失败的情形会导致数据重复。

				//TODO insertIntoDb(buffer);
				System.out.println("insert buffer data to database: " + buffer.size());
				consumer.commitSync();
				buffer.clear();
			}
		}
	}

	/**
	 * 在每个partition上消费完消息之后才提交offset。因为提交的offset是应用程序读取的下一条消息的的offset，所以需要在lastOffset上加1。
	 * @param topic
	 */
	public void consumePartitionWithManualOffsetCommit(String... topic) {
		this.consumer.subscribe(Arrays.asList(topic));
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
			for(TopicPartition partition : records.partitions()) {
				List<ConsumerRecord<String, String>> partitonRecords = records.records(partition);
				partitonRecords.forEach(record -> {
					System.out.printf("topic = %s, offset = %d, key = %s, value = %s%n", record.topic(), record.offset(), record.key(), record.value());
				});
				long lastOffset = partitonRecords.get(partitonRecords.size() - 1).offset();
				consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
			}
		}
	}

	@Override
	public void close() throws Exception {
		consumer.close();
	}

}
