package com.my.project.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

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

	@Override
	public void close() throws Exception {
		producer.close();
	}
}
