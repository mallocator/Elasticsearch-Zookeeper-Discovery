package org.elasticsearch.zookeeper;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

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

	private static final Logger	logger						= LoggerFactory.getLogger(NodeSetMember.class);
	private String				group;
	private String				nodeName;
	private String				nodeValue;
	private ZKConnector			zooConnector;
	private boolean				forceOverwriteExistingNode	= true;
	private boolean				renewNode					= true;
	private boolean				nodeIsExisting				= false;
	private String				nodeLiveValue;

	public void setZooConnector(final ZKConnector zooConnector) {
		this.zooConnector = zooConnector;
	}

	public void setGroup(final String group) {
		this.group = group;
	}

	public void setNodeName(final String nodeName) {
		this.nodeName = nodeName;
	}

	public void setNodeValue(final String nodeValue) {
		this.nodeValue = nodeValue;
	}

	public void setForceOverwriteExistingNode(final boolean forceOverwriteExistingNode) {
		this.forceOverwriteExistingNode = forceOverwriteExistingNode;
	}

	public String getGroup() {
		return this.group;
	}

	public String getNodeName() {
		return this.nodeName;
	}

	public String getNodeValue() {
		return this.nodeValue;
	}

	public boolean isNodeExisting() {
		return this.nodeIsExisting;
	}

	public String getNodeLiveValue() {
		return this.nodeLiveValue;
	}

	public boolean isRenewNode() {
		return this.renewNode;
	}

	public void setRenewNode(final boolean renewNode) {
		this.renewNode = renewNode;
	}

	/**
	 * Checks if everything has been set up correctly and then creates a node in the given group (=path) with the given
	 * value.
	 */
	@PostConstruct
	public final void postConstruct() {
		if (this.zooConnector == null) {
			throw new RuntimeException("'zooConnector' must be set!");
		}
		if (this.group == null) {
			throw new RuntimeException("'group' must bet set!");
		}
		if (this.nodeName == null) {
			throw new RuntimeException("'nodeName' must bet set!");
		}
		if (this.nodeValue == null) {
			throw new RuntimeException("'nodeValue' must bet set!");
		}
		if (!this.group.endsWith("/")) {
			this.group += "/";
		}
		if (this.nodeName.startsWith("/")) {
			this.nodeName = this.nodeName.substring(1);
		}

		try {
			final ZooKeeper zk = this.zooConnector.getZk();
			logger.info("Zookeeper: {} - creating node entry: {}{} = {} ", new Object[] { getConnectionAsString(zk), this.group,
					this.nodeName, this.nodeValue });

			if (this.forceOverwriteExistingNode) {
				final Stat exists = zk.exists(this.group + this.nodeName, false);
				if (exists != null) {
					zk.delete(this.group + this.nodeName, -1);
					logger.info("Zookeeper: {} - existing node entry has been removed: {}{}", new Object[] { getConnectionAsString(zk),
							this.group, this.nodeName });
				}
			}

			zk.create(this.group + this.nodeName, this.nodeValue.getBytes("UTF-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			logger.info("Zookeeper: {} - node entry has been written: {}{} = {} ", new Object[] { getConnectionAsString(zk), this.group,
					this.nodeName, this.nodeValue });

			// starts watching the node
			updateLiveValue();

		} catch (Exception e) {
			logger.error("Zookeeper: Exception while creating ephemeral node", e);
		}
	}

	@PreDestroy
	public final void preDestroy() {
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

	public void registerNode() {
		postConstruct();
	}

	public void unregisterNode() {
		preDestroy();
	}

	public void verifyNode() {
		ZooKeeper zk = this.zooConnector.getZk();

		if (this.renewNode) {

			try {
				if (!zk.getState().isAlive()) {
					logger.info("Zookeeper: {} - trying to reconnect...", new Object[] { getConnectionAsString(zk),
							zk.getState().toString() });
					this.zooConnector.reconnect();
					zk = this.zooConnector.getZk();
					logger.info("Zookeeper: tried to reconnect: {}", getConnectionAsString(zk));
					registerNode();
				}
			} catch (Exception e) {
				logger.error("Zookeeper: {} while reconnection to zookeeper server.", e);
			}

			try {

				final Stat exists = zk.exists(this.group + this.nodeName, false);
				if (exists == null) {
					logger.warn("Zookeeper: {} - node entry is NOT present any more, going to renew it: {}{} = {} ", new Object[] {
							getConnectionAsString(zk), this.group, this.nodeName, this.nodeValue });
					zk.create(this.group + this.nodeName, this.nodeValue.getBytes("UTF-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
					logger.info("Zookeeper: {} - node entry has been written: {}{} = {} ", new Object[] { getConnectionAsString(zk),
							this.group, this.nodeName, this.nodeValue });

					// starts watching the node
					updateLiveValue();
				}
				else {
					logger.info("Zookeeper: {} - checked node entry: node is still existing: {}{} = {} ", new Object[] {
							getConnectionAsString(zk), this.group, this.nodeName, this.nodeLiveValue });
				}

			} catch (Exception e) {
				logger.error("Zookeeper: {} while renewing ephemeral node", e);
			}
		}
	}

	@Override
	public void process(final WatchedEvent we) {
		if (we.getType() == Event.EventType.NodeCreated || we.getType() == Event.EventType.NodeDataChanged) {
			this.nodeIsExisting = true;
			updateLiveValue();
		}
		else if (we.getType() == Event.EventType.NodeDeleted) {
			this.nodeIsExisting = false;
			this.nodeLiveValue = null;
		}
	}

	public void updateLiveValue() {
		String out = null;
		final ZooKeeper zk = this.zooConnector.getZk();
		try {
			byte[] value = zk.getData(this.group + this.nodeName, this, null);
			out = new String(value);
		} catch (KeeperException ex) {
			logger.error("Zookeeper: {} while fetching liveValue for node {}{}.", new Object[] { ex, this.group, this.nodeName });
		} catch (InterruptedException ex) {
			logger.error("Zookeeper: {} while fetching liveValue for node {}{}.", new Object[] { ex, this.group, this.nodeName });
		}

		this.nodeLiveValue = out;
		this.nodeIsExisting = out != null;
	}

	public boolean isConnectionOk() {
		return isConnectionOk(this.zooConnector.getZk());
	}

	public boolean isConnectionOk(final ZooKeeper zk) {
		return zk.getState() == ZooKeeper.States.CONNECTED && zk.getState().isAlive();
	}

	public String getConnectionAsString() {
		return getConnectionAsString(this.zooConnector.getZk());
	}

	public String getConnectionAsString(final ZooKeeper zk) {
		final String tmp = isConnectionOk(zk) ? "OK" : "NO";
		return "connection:" + tmp + " (state:" + zk.getState().toString() + ", isAlive:" + zk.getState().isAlive() + ", zkSessionId:"
				+ zk.getSessionId() + " )";
	}
}
