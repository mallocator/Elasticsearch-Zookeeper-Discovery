package org.elasticsearch.cloud.zk;

import org.elasticsearch.common.inject.AbstractModule;

/**
 * Binds the ZooKeeper service as a module that is available to other modules, like the ZkDiscovery.
 */
public class ZkModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(ZkService.class).asEagerSingleton();
	}
}
