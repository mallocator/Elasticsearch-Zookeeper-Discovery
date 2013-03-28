package org.elasticsearch.zookeeper;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import mockit.Mock;
import mockit.MockClass;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This ZooConnector creates a local server to which to connect to, before opening a connection. All data by the server is
 * deleted, when the ZooConnector is shut down.
 * 
 * @author Ravi.Gairola
 */
@MockClass(realClass = ZKConnector.class)
public class MockZooConnector extends ZKConnector {
	private static final Logger								logger			= LoggerFactory.getLogger(MockZooConnector.class);
	private static final String								SERVER_DATAPATH	= "target/zookeeper/";
	private static final int								SERVER_PORT		= 2181;
	private static final int								DEFAULT_ID		= Integer.MIN_VALUE;
	private static final int								WAIT_FOR_SERVER	= 1000;
	private static final ConcurrentHashMap<Long, ZooKeeper>	clients			= new ConcurrentHashMap<Long, ZooKeeper>();
	private static ZooKeeperServer							server;
	private static final Watcher							watcher			= new Watcher() {
																				@Override
																				public void process(final WatchedEvent event) {}
																			};

	/**
	 * Overrides the standard behavior, so that no initialization is done.
	 */
	@Mock
	public void $init() {}

	/**
	 * Overrides the standard behavior, so that no initialization is done.
	 * 
	 * @param hosts
	 */
	@Mock
	public void $init(final String hosts) {}

	@Mock
	@Override
	public void connect(final String hosts) throws IOException, InterruptedException {
		connect(DEFAULT_ID);
	}

	@Mock
	@Override
	public void close() throws InterruptedException {
		close(DEFAULT_ID);
	}

	@Mock
	@Override
	public void reconnect() throws IOException, InterruptedException {
		reconnect(DEFAULT_ID);
	}

	@Mock
	@Override
	public ZooKeeper getZk() {
		return getZk(DEFAULT_ID);
	}

	public static void connect(final long id) {
		getZk(id);
	}

	public static synchronized void connectServer() {
		if (server == null) {
			logger.info("Launching ZooKeeper server");
			deleteRecursively(new File(SERVER_DATAPATH));
			server = new ZooKeeperServer();
			synchronized (server) {
				try {
					server.setTxnLogFactory(new FileTxnSnapLog(new File(SERVER_DATAPATH + "data"), new File(SERVER_DATAPATH + "snapshots")));
				} catch (IOException e) {
					throw new RuntimeException("Unable to create ZooKeeper data directory  -> aborting server launch", e);
				}
				try {
					server.startup();
				} catch (Exception e) {
					throw new RuntimeException("Unable to launch ZooKeeper server", e);
				}
				waitForServerUp();
			}
		}
	}

	public static void close(final long id) {
		final ZooKeeper keeper = clients.remove(id);
		if (keeper != null) {
			logger.info("Closing ZooKeeper client connection");
			try {
				keeper.close();
			} catch (InterruptedException e) {
				logger.info("Closing ZooKeeper client was interrupted", e);
			}
		}
		if (clients.isEmpty()) {
			closeServer();
		}
	}

	public static void closeAll() {
		for (Entry<Long, ZooKeeper> entry : clients.entrySet()) {
			if (entry.getValue() != null) {
				try {
					entry.getValue().close();
				} catch (InterruptedException e) {
					logger.info("Closing ZooKeeper client was interrupted", e);
				}
			}
		}
		clients.clear();
		closeServer();
	}

	public static synchronized void closeServer() {
		if (server != null) {
			logger.info("Shutting down ZooKeeper server");
			server.getServerCnxnFactory().shutdown();
			waitForServerDown();
			deleteRecursively(new File(SERVER_DATAPATH));
			server = null;
			logger.info("ZooKeeper server shutdown complete");
		}
	}

	public static ZooKeeper getZk(final long id) {
		connectServer();
		if (!clients.containsKey(id)) {
			logger.info("Opening ZooKeeper client connection");
		}
		try {
			clients.putIfAbsent(id, new ZooKeeper("localhost:" + SERVER_PORT, 1000, watcher));
			return clients.get(id);
		} catch (IOException e) {
			throw new RuntimeException("Unable to connect to local ZooKeeper server on port " + SERVER_PORT, e);
		}
	}

	public static Collection<ZooKeeper> getConnectedZooKeepers() {
		return clients.values();
	}

	public static void reconnect(final long id) throws IOException, InterruptedException {
		close(id);
		connect(id);
	}

	private static String send4LetterWord(final String host, final int port, final String cmd) throws IOException {
		final Socket sock = new Socket(host, port);
		BufferedReader reader = null;
		try {
			final OutputStream outstream = sock.getOutputStream();
			outstream.write(cmd.getBytes());
			outstream.flush();
			sock.shutdownOutput();

			reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
			final StringBuilder sb = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				sb.append(line + "\n");
			}
			return sb.toString();
		} finally {
			sock.close();
			if (reader != null) {
				reader.close();
			}
		}
	}

	private static boolean waitForServerUp() {
		final long start = System.currentTimeMillis();
		while (true) {
			try {
				final String result = send4LetterWord("localhost", SERVER_PORT, "stat");
				if (result.startsWith("Zookeeper version:") && !result.contains("READ-ONLY")) {
					return true;
				}
			} catch (IOException e) {
				logger.trace("Server threw IOException, probably because it's not yet started", e);
			}

			if (System.currentTimeMillis() > start + WAIT_FOR_SERVER) {
				throw new RuntimeException("Server was unable to start up in time (time limit = " + WAIT_FOR_SERVER + "ms)");
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				logger.trace("Thread sleep failed", e);
			}
		}
	}

	private static boolean waitForServerDown() {
		final long start = System.currentTimeMillis();
		while (true) {
			try {
				send4LetterWord("localhost", SERVER_PORT, "stat");
			} catch (IOException e) {
				return true;
			}
			if (System.currentTimeMillis() > start + WAIT_FOR_SERVER) {
				throw new RuntimeException("Server was unable to shut down in time (time limit = " + WAIT_FOR_SERVER + "ms)");
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				logger.trace("Thread sleep failed", e);
			}
		}
	}

	private static void deleteRecursively(final File dir) {
		if (dir == null || !dir.exists()) {
			return;
		}
		for (final File file : dir.listFiles()) {
			if (file.isDirectory()) {
				deleteRecursively(file);
			}
			if (!file.delete()) {
				MockZooConnector.logger.warn("Couldn't delete data during cleanup: {}", file);
			}
		}
	}
}
