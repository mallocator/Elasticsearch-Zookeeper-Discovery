package org.elasticsearch.plugin.cloud.zk;

import java.util.Collection;

import org.elasticsearch.cloud.zk.ZkModule;
import org.elasticsearch.cloud.zk.ZkService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;

/**
 * Registers the discovery module with elastic search.
 */
public class CloudZkPlugin extends AbstractPlugin {

	private final Settings	settings;

	public CloudZkPlugin(final Settings settings) {
		this.settings = settings;
	}

	@Override
	public String name() {
		return "cloud-zk";
	}

	@Override
	public String description() {
		return "Cloud ZooKeeper Plugin";
	}

	@Override
	public Collection<Class<? extends Module>> modules() {
		Collection<Class<? extends Module>> modules = Lists.newArrayList();
		if (this.settings.getAsBoolean("cloud.zk.enabled", false)) {
			modules.add(ZkModule.class);
		}
		return modules;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Collection<Class<? extends LifecycleComponent>> services() {
		Collection<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
		if (this.settings.getAsBoolean("cloud.zk.enabled", false)) {
			services.add(ZkService.class);
		}
		return services;
	}
}
