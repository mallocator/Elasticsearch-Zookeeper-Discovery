package org.elasticsearch.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeSetMember implements Watcher {
	private static final Logger	logger	= LoggerFactory.getLogger(NodeSetMember.class);
	private String				group;
	private String				nodeName;
	private final String		nodeValue;
	private final ZKConnector	zooConnector;

	public NodeSetMember(final ZKConnector zooConnector, final String group, final String nodeName, final String value) {
		this.zooConnector = zooConnector;
		this.group = group;
		this.nodeName = nodeName;
		this.nodeValue = value;
	}

	/**
	 * Checks if everything has been set up correctly and then creates a node in the given group (=path) with the given
	 * value.
	 */
	public void registerNode() {
		if (!this.group.endsWith("/")) {
			this.group += "/";
		}
		if (this.nodeName.startsWith("/")) {
			this.nodeName = this.nodeName.substring(1);
		}

		try {
			final ZooKeeper zk = this.zooConnector.getZk();
			logger.info("Zookeeper: {} - creating node entry: {}{} = {} ", new Object[] { getConnectionAsString(), this.group,
					this.nodeName, this.nodeValue });

			final Stat exists = zk.exists(this.group + this.nodeName, false);
			if (exists != null) {
				zk.delete(this.group + this.nodeName, -1);
				logger.info("Zookeeper: {} - existing node entry has been removed: {}{}", new Object[] { getConnectionAsString(),
						this.group, this.nodeName });
			}

			zk.create(this.group + this.nodeName, this.nodeValue.getBytes("UTF-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			logger.info("Zookeeper: {} - node entry has been written: {}{} = {} ", new Object[] { getConnectionAsString(), this.group,
					this.nodeName, this.nodeValue });

			watchZKNode();
		} catch (Exception e) {
			logger.error("Zookeeper: Exception while creating ephemeral node", e);
		}
	}

	/**
	 * Removes the node from Zookeeper.
	 */
	public void unregisterNode() {
		try {
			final ZooKeeper zk = this.zooConnector.getZk();
			zk.delete(this.group + this.nodeName, -1);
		} catch (InterruptedException ex) {
			logger.warn("Interruption while removing Zookeeper node entry in preDestroy {}{}. (Nevertheless zookeeper should removed this node after some secondes.)",
				new Object[] { this.group, this.nodeName },
				ex);
		} catch (KeeperException ex) {
			logger.warn("KeeperProblem while removing Zookeeper node entry in preDestroy " + this.group + this.nodeName
					+ ". (Nevertheless zookeeper should removed this node after some secondes.)", ex);
		}
		logger.info("Removed Zookeeper node entry in preDestroy {}{}", new Object[] { this.group, this.nodeName });
	}

	@Override
	public void process(final WatchedEvent we) {
		if (we.getType() == Event.EventType.NodeCreated || we.getType() == Event.EventType.NodeDataChanged) {
			watchZKNode();
		}
	}

	private void watchZKNode() {
		final ZooKeeper zk = this.zooConnector.getZk();
		try {
			zk.getData(this.group + this.nodeName, this, null);
		} catch (KeeperException ex) {
			logger.error("Zookeeper: {} while fetching liveValue for node {}{}.", new Object[] { ex, this.group, this.nodeName });
		} catch (InterruptedException ex) {
			logger.error("Zookeeper: {} while fetching liveValue for node {}{}.", new Object[] { ex, this.group, this.nodeName });
		}
	}

	private String getConnectionAsString() {
		final ZooKeeper zk = this.zooConnector.getZk();
		final String tmp = isConnectionOk() ? "OK" : "NO";
		return "connection:" + tmp + " (state:" + zk.getState().toString() + ", isAlive:" + zk.getState().isAlive() + ", zkSessionId:"
				+ zk.getSessionId() + " )";
	}

	private boolean isConnectionOk() {
		final ZooKeeper zk = this.zooConnector.getZk();
		return zk.getState() == ZooKeeper.States.CONNECTED && zk.getState().isAlive();
	}
}
