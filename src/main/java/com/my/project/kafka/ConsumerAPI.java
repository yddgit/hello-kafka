package com.my.project.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

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

	/**
	 * Manual Partition Assignment
	 * 
	 * 需要手动指定partition的场合：
	 * 1. 如果处理包含关联partition的一些本地状态，就需要从partition的本地磁盘获取数据
	 * 2. 如果处理程序是支持HA的，当结点失效后会自动重启，这时就不需要kafka检测失效并重新分配partition，
	 * 因为consumer会自动在别一台机器上重启并接管其原来分配到的partition。
	 * 
	 * 调用assign方法开始消费，手动分配后就不会使用自动分配了，通常要保证每个consumer的group.id是唯一的。
	 * 不能将手动分配partition和自动分配partition混合使用。
	 * 
	 * @param topic
	 */
	public void consumeWithManualPartitionAssignment(String topic, int partition) {
		TopicPartition partition0 = new TopicPartition(topic, partition);
		consumer.assign(Arrays.asList(partition0));
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for(ConsumerRecord<String, String> record : records) {
				System.out.printf("topic = %s, offset = %d, key = %s, value = %s%n", record.topic(), record.offset(), record.key(), record.value());
			}
		}
	}

	/**
	 * 对于对时间敏感的消息处理程序，如果已经落后了很多条消息，而且也不想处理所有的数据记录，那就直接跳到最近的消息位置即可。
	 * 
	 * 还有一个场景是应用程序维护了一些本地状态，这类应用需要在启动时就从本地存储中初始化position。
	 * 同样如果本地状态被销毁(如磁盘数据丢失)，那可以在新的机器上重新消费所有数据重建本地状态(假设Kakfa保留了足够的历史数据)
	 * 
	 * Kafka使用seek(TopicPartition, long)方法指定新的position。特别的，跳转到开始/最新的offset使用如下两个方法：
	 * seekToBeginning(Collection)和seekToEnd(Collection)
	 * 
	 * @param topic
	 */
	public void consumeSeekToBeginOrEnd(String... topic) {
		this.consumer.subscribe(Arrays.asList(topic));
		boolean needReConsume = true;
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			Set<TopicPartition> partitions = records.partitions();
			if(needReConsume) {
				this.consumer.seekToBeginning(partitions);
				// this.consumer.seekToEnd(partitions);
				needReConsume = false;
			}
			for(TopicPartition partition : partitions) {
				List<ConsumerRecord<String, String>> partitonRecords = records.records(partition);
				partitonRecords.forEach(record -> {
					System.out.printf("topic = %s, offset = %d, key = %s, value = %s%n", record.topic(), record.offset(), record.key(), record.value());
				});
			}
		}
	}

	@Override
	public void close() throws Exception {
		consumer.close();
	}

}
