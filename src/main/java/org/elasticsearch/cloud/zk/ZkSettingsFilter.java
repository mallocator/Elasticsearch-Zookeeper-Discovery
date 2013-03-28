package org.elasticsearch.cloud.zk;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.SettingsFilter;

/**
 * Removes settings from the settings filter so that they are not processed by other plugins.
 */
public class ZkSettingsFilter implements SettingsFilter.Filter {
	@Override
	public void filter(final ImmutableSettings.Builder settings) {
		settings.remove("zk.path");
		settings.remove("zk.hosts");
		settings.remove("zk.hostname");
	}
}
