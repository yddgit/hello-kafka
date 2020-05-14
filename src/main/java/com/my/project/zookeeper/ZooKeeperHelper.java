package com.my.project.zookeeper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * Hello ZooKeeper
 * @author yang
 */
public class ZooKeeperHelper implements Watcher, Closeable {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private static CountDownLatch connected = new CountDownLatch(1);

	private ZooKeeper zoo;

	public ZooKeeperHelper(String connect) {
		try {
			zoo = new ZooKeeper(connect, 5000, this);
			logger.info("zookeeper state: " + zoo.getState());
		} catch (IOException e) {
			logger.error("zookeeper connect error", e);
		}

		try {
			connected.await();
		} catch (InterruptedException e) {
			logger.error("wait zookeeper connect error", e);
		}
	}

	/**
	 * 递归删除znode
	 * @param path znode path
	 * @throws Exception
	 */
	public void delete(String path) throws Exception {
		PathUtils.validatePath(path);
		Deque<String> queue = new LinkedList<String>();
		List<String> tree = new ArrayList<String>();
		queue.add(path);
		tree.add(path);
		while (true) {
			String node = queue.pollFirst();
			if (node == null) {
				break;
			}
			List<String> children = zoo.getChildren(node, false);
			for (final String child : children) {
				final String childPath = node + "/" + child;
				queue.add(childPath);
				tree.add(childPath);
			}
		}
		logger.info("Deleting " + tree);
		logger.info("Deleting " + tree.size() + " subnodes");
		for(int i = tree.size() - 1; i >= 0; --i) {
			zoo.delete(tree.get(i), -1);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		logger.info("watched event: " + event);
		if (event.getState() == KeeperState.SyncConnected) {
			connected.countDown();
			logger.info("zookeeper connected");
		}
	}

	@Override
	public void close() throws IOException {
		try {
			zoo.close();
		} catch (InterruptedException e) {
			logger.error("zookeeper close error", e);
		}
	}

}
