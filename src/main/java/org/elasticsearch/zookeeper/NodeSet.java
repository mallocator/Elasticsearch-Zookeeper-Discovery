package org.elasticsearch.zookeeper;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
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
 * 
 * @param <T> The data type that should be stored in the ZooKeeper Node
 */
public class NodeSet<T> implements Watcher, Iterable<Entry<String, T>> {
	private static final Logger	logger	= LoggerFactory.getLogger(NodeSet.class);

	/**
	 * Interface used to deserialize the data that was stored in the ZooKeeper Node. Clients that want to retrieve data that
	 * they stored need to implement this interface.
	 * 
	 * @param <T>
	 */
	public interface NodeContentDeserializer<T> {
		/**
		 * The method that deserializes the byte[] information from the ZooKeeper Node into whatever it was before.
		 * 
		 * @param data
		 * @return
		 */
		T deserialize(byte[] data);
	}

	private final ZooKeeper						zoo;
	private final String						groupPath;
	private final Map<String, T>				nodeMap	= new ConcurrentHashMap<String, T>();
	private final NodeContentDeserializer<T>	ser;
	private final Random						rand;

	public NodeSet(final ZKConnector zoo, final String groupPath, final NodeContentDeserializer<T> ser) {
		this(zoo.getZk(), groupPath, ser);
	}

	public NodeSet(final ZooKeeper zk, final String groupPath, final NodeContentDeserializer<T> ser) {
		this.zoo = zk;
		this.groupPath = groupPath;
		this.ser = ser;
		this.rand = new Random();
		try {
			getNodesFromZoo();
		} catch (Exception e) {
			logger.warn("Exception while processing watch", e);
		}
	}

	private synchronized void getNodesFromZoo() throws KeeperException, InterruptedException {
		try {
			final Set<String> newState = new HashSet<String>(this.zoo.getChildren(this.groupPath, this));
			final Set<String> toDelete = new HashSet<String>(this.nodeMap.keySet());
			toDelete.removeAll(newState);

			final Set<String> toAdd = new HashSet<String>(newState);
			newState.removeAll(this.nodeMap.keySet());

			for (String node : toDelete) {
				remove(node);
			}
			for (String node : toAdd) {
				add(node);
			}
		} catch (KeeperException.NoNodeException e) {
			throw new RuntimeException("Group does not exist: " + this.groupPath, e);
		}
	}

	private void add(final String node) throws KeeperException, InterruptedException {
		final byte[] data = this.zoo.getData(this.groupPath + "/" + node, this, null);
		final T meta = this.ser.deserialize(data);
		this.nodeMap.put(node, meta);
	}

	private void remove(final String node) {
		this.nodeMap.remove(node);
	}

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

	/**
	 * Returns on random element of the data set held under this set of ZooKeeper nodes.
	 * 
	 * @return
	 */
	public T getRandomMeta() {
		if (this.nodeMap.size() > 0) {
			final int r = this.rand.nextInt(this.nodeMap.size());
			int i = 0;
			for (T meta : this.nodeMap.values()) {
				if (i == r) {
					return meta;
				}
				i++;
			}
		}
		return null;
	}

	@Override
	public Iterator<Entry<String, T>> iterator() {
		return this.nodeMap.entrySet().iterator();
	}

	/**
	 * Returns the data of the node in this set of nodes with the given id.
	 * 
	 * @param nodeId
	 * @return
	 */
	public T getByName(final String nodeId) {
		return this.nodeMap.get(nodeId);
	}
}
