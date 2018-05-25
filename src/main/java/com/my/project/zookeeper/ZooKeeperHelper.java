package com.my.project.zookeeper;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
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
	 * 删除znode
	 * @param path znode path
	 * @throws Exception
	 */
	public void delete(String path) throws Exception {
		deleteRecursive(zoo, path);
	}

	/**
	 * 递归删除zookeeper结点
	 * @param zoo ZooKeeper
	 * @param path 结点path
	 * @throws Exception
	 */
	private void deleteRecursive(ZooKeeper zoo, String path) throws Exception {
		List<String> children = zoo.getChildren(path, false);
		if(children != null && children.size() > 0) {
			for(String childPath : children) {
				deleteRecursive(zoo, path + "/" + childPath);
			}
		} else {
			logger.info(path);
			if(zoo.exists(path, false) != null) {
				zoo.delete(path, -1);
			}
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
