package org.elasticsearch.cloud.zk;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.zookeeper.NodeSet;
import org.elasticsearch.zookeeper.NodeSet.NodeContentDeserializer;
import org.elasticsearch.zookeeper.NodeSetMember;
import org.elasticsearch.zookeeper.ZKConnector;

/**
 * This service establishes the actual connection to the ZooKeeper and finds the other nodes of the cluster.
 */
public class ZkService extends AbstractLifecycleComponent<ZkService> {
	private final ZKConnector		zooConnector;
	private final NodeSet<String>	nodes;
	private final String			zkPath;
	private NodeSetMember			groupMember;

	@Inject
	public ZkService(final Settings settings, final SettingsFilter settingsFilter) {
		super(settings);
		settingsFilter.addFilter(new ZkSettingsFilter());

		final StringBuilder hosts = new StringBuilder();
		for (String host : settings.getAsArray("cloud.zk.hosts")) {
			hosts.append(",").append(host);
		}
		if (hosts.length() > 1) {
			this.zooConnector = new ZKConnector(hosts.substring(1));
		}
		else {
			this.logger.error("ZooKeeper Service initialisation failed (hosts: {})", settings.get("cloud.zk.hosts"));
			throw new RuntimeException("ZooKeeper Service initialisation has failed - no hosts were supplied");
		}

		this.zkPath = settings.get("cloud.zk.path", "/elasticsearch");
		this.nodes = new NodeSet<String>(this.zooConnector, this.zkPath, new NodeContentDeserializer<String>() {
			@Override
			public String deserialize(final byte[] data) {
				try {
					return new String(data, "UTF-8");
				} catch (Exception e) {
					ZkService.this.logger.error("Unable to deserialize server from zookeeper");
					return null;
				}
			}
		});
	}

	@Override
	protected void doStart() throws ElasticSearchException {}

	@Override
	protected void doStop() throws ElasticSearchException {
		unregisterNode();
	}

	@Override
	protected void doClose() throws ElasticSearchException {
		unregisterNode();
	}

	public ZKConnector getZooConnector() {
		return this.zooConnector;
	}

	public NodeSet<String> getNodes() {
		return this.nodes;
	}

	/**
	 * Registers a node at a specified ZooKeeper path, so that other nodes can find this node.
	 * 
	 * @param myAddress
	 */
	public void registerNode(final String myAddress) {
		if (this.groupMember != null) {
			this.logger.info("Already registered with ZooKeeper, skipping registration");
			return;
		}
		this.groupMember = new NodeSetMember();
		this.groupMember.setZooConnector(this.zooConnector);
		this.groupMember.setGroup(this.zkPath);
		this.groupMember.setNodeName(getZKNodeName());
		this.groupMember.setNodeValue(myAddress);
		this.groupMember.setForceOverwriteExistingNode(true);
		this.groupMember.setRenewNode(true);
		this.groupMember.postConstruct();
		this.logger.info("Registered with ZooKeeper under node {} with address {}", getZKNodeName(), myAddress);
	}

	private void unregisterNode() {
		if (this.groupMember != null) {
			try {
				this.zooConnector.getZk().delete(this.zkPath + "/" + getZKNodeName(), -1);
				this.groupMember = null;
			} catch (Exception e) {
				this.logger.info("Unable to delete connection node from ZooKeeper");
			}
		}
	}

	private String getZKNodeName() {
		return nodeName().replaceAll("[,|\\.| |']", "").trim();
	}
}
