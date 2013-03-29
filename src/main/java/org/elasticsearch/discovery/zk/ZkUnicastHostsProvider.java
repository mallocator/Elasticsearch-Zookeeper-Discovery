package org.elasticsearch.discovery.zk;

import java.net.InetAddress;
import java.util.List;
import java.util.Map.Entry;

import org.elasticsearch.cloud.zk.ZkService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;

/**
 * Is used to register this node and create a list of available nodes in the cluster.
 */
public class ZkUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {
	private final TransportService	transportService;
	private final ZkService			zkService;
	private final String			hostname;

	public ZkUnicastHostsProvider(final Settings settings, final TransportService transportService, final ZkService zkService) {
		super(settings);
		this.hostname = settings.get("cloud.zk.hostname", "");
		this.zkService = zkService;
		this.transportService = transportService;
	}

	@Override
	public List<DiscoveryNode> buildDynamicNodes() {
		this.logger.info("Building list of dynamic discovery nodes from ZooKeeper");
		final String myAddress = getMyAddress();
		this.zkService.setNodeAddress(myAddress);

		final List<DiscoveryNode> discoNodes = Lists.newArrayList();
		int clientCount = 0;
		for (Entry<String, String> entry : this.zkService.getNodes()) {
			if (entry.getValue().equals(myAddress)) {
				continue;
			}
			clientCount++;
			try {
				int i = 0;
				for (TransportAddress address : this.transportService.addressesFromString(entry.getValue())) {
					this.logger.debug("Found node \"{}\" with address {}", entry.getKey(), address);
					discoNodes.add(new DiscoveryNode("#cloud-" + entry.getKey() + "-" + i++, address));
				}
			} catch (Exception e) {
				this.logger.warn("Can't add address {} as valid DiscoveryNode", entry.getValue());
			}
		}
		this.logger.info("Found {} other nodes via ZooKeeper", clientCount);

		return discoNodes;
	}

	private String getMyAddress() {
		if (!this.hostname.isEmpty()) {
			return this.hostname;
		}
		this.logger.info("Hostname has not been set - autodetecting my address");
		String myIpAddress = this.transportService.boundAddress().publishAddress().toString();
		myIpAddress = myIpAddress.substring(6, myIpAddress.length() - 1);

		String address = null;
		try {
			address = InetAddress.getLocalHost().getCanonicalHostName();
			if (!address.equals("localhost")) {
				return address + ":" + myIpAddress.split(":")[1];
			}
		} catch (Exception e) {
			this.logger.info("Can't find FQDN, falling back to IP");
		}
		return myIpAddress;
	}
}
