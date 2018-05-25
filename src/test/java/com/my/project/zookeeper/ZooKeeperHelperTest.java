package com.my.project.zookeeper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZooKeeperHelperTest {

	private ZooKeeperHelper helper;

	@Before
	public void setUp() throws Exception {
		helper = new ZooKeeperHelper("localhost:2181");
	}

	@After
	public void tearDown() throws Exception {
		helper.close();
	}

	@Test
	public void testDelete() throws Exception {
		helper.delete("/kafka");
	}

}
