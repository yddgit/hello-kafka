package com.my.project.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminAPI implements AutoCloseable {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private AdminClient client;

	public AdminAPI(Properties config) {
		this.client = AdminClient.create(config);
	}

	/**
	 * 创建topic
	 * @param topics topic name
	 */
	public void createTopics(String... topics) {
		try {
			List<NewTopic> newTopics = new ArrayList<NewTopic>();
			Arrays.asList(topics).forEach(s -> {
				newTopics.add(new NewTopic(s, 1, (short)3));
			});
			client.createTopics(newTopics).all().get();
		} catch (Exception e) {
			logger.error("create topic error: " + topics, e);
		}
	}

	/**
	 * 列出所有topic
	 */
	public void listTopics() {
		try {
			ListTopicsResult result = client.listTopics();
			StringBuffer buffer = new StringBuffer("Topics on kafka cluster:");
			result.listings().get().forEach(e -> {
				buffer.append("\n\t").append(e.toString());
			});
			logger.info(buffer.toString());
		} catch (Exception e) {
			logger.error("list topic error!", e);
		}
	}

	/**
	 * 查看topic详情
	 * @param topics topic name
	 */
	public void describeTopics(String... topics) {
		try {
			StringBuffer buffer = new StringBuffer("Topics on kafka cluster:");
			client.describeTopics(Arrays.asList(topics)).all().get()
				.forEach((key, value) -> {
					buffer.append("\n\t").append(value.toString());
				});
			logger.info(buffer.toString());
		} catch (Exception e) {
			logger.error("describe topic error: " + topics, e);
		}
	}

	/**
	 * 删除topic
	 * @param topics topic name
	 */
	public void deleteTopics(String... topics) {
		try {
			client.deleteTopics(Arrays.asList(topics)).all().get();
		} catch (Exception e) {
			logger.error("delete topic error: " + topics, e);
		}
	}

	@Override
	public void close() throws Exception {
		this.client.close();
	}
}
