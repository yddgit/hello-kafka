package com.my.project.kafka;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AdminAPITest {

	private AdminAPI admin;

	@Before
	public void setUp() throws Exception {
		Properties config = new Properties();
		config.put("bootstrap.servers", "192.168.56.10:9092");
		admin = new AdminAPI(config);
	}

	@After
	public void tearDown() throws Exception {
		admin.close();
	}

	@Test
	public void testList() {
		admin.listTopics();
	}

	@Test
	public void testCreate() {
		admin.createTopics("test", "my-topic");
	}

	@Test
	public void testDescribe() {
		admin.describeTopics("test", "my-topic");
	}
}
