package com.my.project.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

public class ProducerAPI implements AutoCloseable {

	private Producer<String, String> producer;

	public ProducerAPI(Properties config) {
		this.producer = new KafkaProducer<>(config);
	}

	/**
	 * 生产消息
	 * @param topic topic name
	 * @param record data
	 */
	public void produce(String topic, Map<String, String> record) {
		if(record != null && record.size() > 0) {
			record.forEach((key, value) -> {
				producer.send(new ProducerRecord<String, String>(topic, key, value));
			});
		}
	}

	/**
	 * 生产消息增加事务控制
	 * @param topic topic name
	 * @param record data
	 */
	public void produceWithTransaction(String topic, Map<String, String> record) {
		if(record != null && record.size() > 0) {
			producer.initTransactions();
			try {
			    producer.beginTransaction();
			    record.forEach((key, value) -> {
			    	producer.send(new ProducerRecord<String, String>(topic, key, value));
			    });
			    producer.commitTransaction();
			} catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
			    // We can't recover from these exceptions, so our only option is to close the producer and exit.
			    producer.close();
			} catch (KafkaException e) {
			    // For all other exceptions, just abort the transaction and try again.
			    producer.abortTransaction();
			}
		}
	}

	@Override
	public void close() throws Exception {
		producer.close();
	}
}
