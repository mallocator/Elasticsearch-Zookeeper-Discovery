package org.elasticsearch.zookeeper;

import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores a set of data in a set of ZooKeeper nodes for later retrieval. Data is only kept as long as the client that set the
 * value is connected.
 */
public class NodeSet<T> implements Watcher, Iterable<Entry<String, String>> {
	private static final Logger			logger	= LoggerFactory.getLogger(NodeSet.class);
	private final ZooKeeper				zoo;
	private final String				groupPath;
	private final Map<String, String>	nodeMap	= new ConcurrentHashMap<String, String>();

	public NodeSet(final ZKConnector zoo, final String groupPath) {
		this.zoo = zoo.getZk();
		this.groupPath = groupPath;
		try {
			getNodesFromZoo();
		} catch (Exception e) {
			logger.warn("Exception while processing watch", e);
		}
	}

	/**
	 * React on an event fired by the ZooKeeper Cluster and update our known information.
	 */
	@Override
	public void process(final WatchedEvent event) {
		if (event.getType() == EventType.NodeChildrenChanged) {
			try {
				getNodesFromZoo();
			} catch (Exception e) {
				logger.warn("Exception while processing watch", e);
			}

		}
		else if (event.getType() == EventType.NodeDataChanged) {
			final String path = event.getPath();
			final String[] compStrings = path.split("/");
			final String node = compStrings[compStrings.length - 1];

			try {
				remove(node);
				add(node);
			} catch (Exception e) {
				logger.warn("Exception while processing watch", e);
			}
		}
	}

	@Override
	public Iterator<Entry<String, String>> iterator() {
		return this.nodeMap.entrySet().iterator();
	}

	/**
	 * Fetches data form ZooKeeper and checks what information needs to be updated.
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws UnsupportedEncodingException
	 */
	private synchronized void getNodesFromZoo() throws KeeperException, InterruptedException, UnsupportedEncodingException {
		try {
			final Set<String> newState = new HashSet<String>(this.zoo.getChildren(this.groupPath, this));
			final Set<String> toDelete = new HashSet<String>(this.nodeMap.keySet());
			toDelete.removeAll(newState);

			final Set<String> toAdd = new HashSet<String>(newState);
			newState.removeAll(this.nodeMap.keySet());

			for (final String node : toDelete) {
				remove(node);
			}
			for (final String node : toAdd) {
				add(node);
			}
		} catch (KeeperException.NoNodeException e) {
			throw new RuntimeException("Group does not exist: " + this.groupPath, e);
		}
	}

	private void add(final String node) throws KeeperException, InterruptedException, UnsupportedEncodingException {
		final byte[] data = this.zoo.getData(this.groupPath + "/" + node, this, null);
		this.nodeMap.put(node, new String(data, "UTF-8"));
	}

	private void remove(final String node) {
		this.nodeMap.remove(node);
	}
}
