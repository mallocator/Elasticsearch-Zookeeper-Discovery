package org.elasticsearch.discovery.zk;

import org.elasticsearch.cloud.zk.ZkService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.discovery.zen.ping.ZenPingService;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Registers ZooKeeper discovery as a ping resolver.
 */
public class ZkDiscovery extends ZenDiscovery {
	@Inject
	public ZkDiscovery(final Settings settings, final ClusterName clusterName, final ThreadPool threadPool,
			final TransportService transportService, final ClusterService clusterService, final NodeSettingsService nodeSettingsService,
			final DiscoveryNodeService discoveryNodeService, final ZenPingService pingService, final ZkService ec2Service) {
		super(settings, clusterName, threadPool, transportService, clusterService, nodeSettingsService, discoveryNodeService, pingService);

		this.logger.info("Setting up ZkDiscovery");
		if (settings.getAsBoolean("cloud.zk.enabled", false)) {
			final ImmutableList<? extends ZenPing> zenPings = pingService.zenPings();
			UnicastZenPing unicastZenPing = null;
			for (ZenPing zenPing : zenPings) {
				if (zenPing instanceof UnicastZenPing) {
					unicastZenPing = (UnicastZenPing) zenPing;
					break;
				}
			}
			if (unicastZenPing != null) {
				this.logger.info("Added ZkUnicastHostsProvider to zen pings");
				unicastZenPing.addHostsProvider(new ZkUnicastHostsProvider(settings, transportService, ec2Service));
				pingService.zenPings(ImmutableList.of(unicastZenPing));
			}
			else {
				this.logger.warn("failed to apply zk unicast discovery, no unicast ping found");
			}
		}
	}
}
