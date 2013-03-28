package org.elasticsearch.zookeeper;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to list information about paths and their children.
 */
public final class ZKPathUtils {
	private ZKPathUtils() {}

	private static final Logger	logger	= LoggerFactory.getLogger(ZKPathUtils.class);

	/**
	 * Returns a list of direct children for the given path.
	 * 
	 * @param zoo
	 * @param path
	 * @return
	 */
	public static List<String> getChildren(final ZooKeeper zoo, final String path) {
		try {
			return zoo.getChildren(path, false);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}

	/**
	 * Returns the direct number of children for a given path.
	 * 
	 * @param zoo
	 * @param path
	 * @return
	 */
	public static int getNumChildren(final ZooKeeper zoo, final String path) {
		try {
			return zoo.getChildren(path, false).size();
		} catch (KeeperException ke) {
			if (ke.code() == Code.NONODE) {
				return -1;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return -1;
	}

	/**
	 * List all paths with up to 20 children in the root path.
	 * 
	 * @see ZKPathUtils#listChildren(ZooKeeper, String, int)
	 * @param zoo
	 */
	public static void listChildren(final ZooKeeper zoo) {
		listChildren(zoo, "/", 20);
	}

	/**
	 * List all children in all sub-directories for the given path. If the number of child nodes exceeds maxChildren no
	 * further child nodes in that path will be analyzed. This value also limits the number of children that will have their
	 * name sent to the output.
	 * 
	 * @param zoo
	 * @param path
	 * @param maxChildren
	 */
	public static void listChildren(final ZooKeeper zoo, final String path, final int maxChildren) {
		for (String child : getChildren(zoo, path)) {
			if (child != null) {
				final int childCount = getNumChildren(zoo, formatPath(path, child));
				logger.debug("path {} has {} children", formatPath(path, child), childCount);
				listFirstChildnames(zoo, formatPath(path, child), maxChildren);
				if (childCount < maxChildren) {
					listChildren(zoo, formatPath(path, child), maxChildren);
				}
				else {
					logger.debug("too many children for path {} childCount {}", formatPath(path, child), childCount);
				}
			}
		}
	}

	/**
	 * List the first n direct children of a path.
	 * 
	 * @param zoo
	 * @param path
	 * @param maxChildNames
	 */
	public static void listFirstChildnames(final ZooKeeper zoo, final String path, final int maxChildNames) {
		int count = 0;
		for (String child : getChildren(zoo, path)) {
			logger.debug("list first {} child names for path {}", maxChildNames, formatPath(path, child));
			count++;
			if (count > maxChildNames) {
				break;
			}
		}
	}

	/**
	 * Format a path so that Zookeeper accepts the input.
	 * 
	 * @param parent
	 * @param child
	 * @return
	 */
	public static String formatPath(final String parent, final String child) {
		return parent.endsWith("/") ? parent + child : parent + "/" + child;
	}

	/**
	 * Recursively deletes a path and all its sub nodes.
	 * 
	 * @param zk
	 * @param path
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public static void recursiveDelete(final ZooKeeper zk, final String path) throws InterruptedException, KeeperException {
		final Stat childStat = zk.exists(path, false);
		if (childStat == null) {
			return;
		}
		for (final String child : zk.getChildren(path, false)) {
			recursiveDelete(zk, formatPath(path, child));
		}
		logger.trace("Deleting path {}", path);
		zk.delete(path, -1);
	}
}
