package org.elasticsearch.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * A wrapper class for the {@link ZooKeeper} that tries to build up a connection for a certain time, before timing out.
 */
public class ZKConnector implements Watcher {

	private static final int		sessionTimeout	= 30000;
	private ZooKeeper				zk;
	private final CountDownLatch	connectedSignal	= new CountDownLatch(1);
	private String					hosts;

	/**
	 * Creates an instance without connecting, or doing anything else.
	 */
	public ZKConnector() {}

	/**
	 * Creates an instance and immediately connects to the given hosts.
	 * 
	 * @param hosts Comma seperated connectString for ZooKeeper. See {@link ZooKeeper#ZooKeeper(String, int, Watcher)}
	 */
	public ZKConnector(final String hosts) {
		try {
			connect(hosts);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Connects to the ZooKeeper server via the given connect String and waits for {@link KeeperState}.SyncConnected for a
	 * certain time.
	 * 
	 * @param hosts
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public synchronized void connect(final String hosts) throws IOException, InterruptedException {
		this.hosts = hosts;
		this.zk = new ZooKeeper(hosts, sessionTimeout, this);
		this.connectedSignal.await();
	}

	@Override
	public void process(final WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			this.connectedSignal.countDown();
		}
	}

	/**
	 * Closes the current connection to the ZooKeeper server.
	 * 
	 * @see ZooKeeper#close();
	 * @throws InterruptedException
	 */
	public synchronized void close() throws InterruptedException {
		this.zk.close();
	}

	public ZooKeeper getZk() {
		return this.zk;
	}

	/**
	 * Tries to {@link ZKConnector#close()} and {@link ZKConnector#connect()} to a ZooKeeper server.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public synchronized void reconnect() throws IOException, InterruptedException {
		try {
			this.zk.close();

		} catch (Exception e) {
			// TODO: handle exception
		}
		connect(this.hosts);

	}
}