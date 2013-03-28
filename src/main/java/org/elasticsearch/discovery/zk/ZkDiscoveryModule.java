package org.elasticsearch.discovery.zk;

import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.ZenDiscoveryModule;

/**
 * A wrapper to bind the ZooKeeper discovery as a singleton to the available discovery modules.
 */
public class ZkDiscoveryModule extends ZenDiscoveryModule {
	@Override
	protected void bindDiscovery() {
		bind(Discovery.class).to(ZkDiscovery.class).asEagerSingleton();
	}
}
