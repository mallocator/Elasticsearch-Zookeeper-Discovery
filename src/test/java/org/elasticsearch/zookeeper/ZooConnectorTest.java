package org.elasticsearch.zookeeper;

import mockit.Mockit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ZooConnectorTest {
	private final Logger	logger	= LoggerFactory.getLogger(getClass());

	@BeforeClass
	public void setUpMocks() {
		Mockit.setUpMock(new MockZooConnector());
	}

	@Test
	public void testMockZooConnectorCleanupBetweenInvokations() throws Exception {
		final ZKConnector zoo = new ZKConnector();
		zoo.connect(null);
		this.logger.info("Stat of test path before create: {}", zoo.getZk().exists("/test", null));
		zoo.getZk().create("/test", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		this.logger.info("Stat of test path: {}", zoo.getZk().exists("/test", null));
		zoo.close();

		zoo.connect(null);
		this.logger.info("Stat of test path before create: {}", zoo.getZk().exists("/test", null));
		zoo.getZk().create("/test", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		this.logger.info("Stat of test path: {}", zoo.getZk().exists("/test", null));
		zoo.getZk().delete("/test", -1);
		this.logger.info("Stat of test path after delete: {}", zoo.getZk().exists("/test", null));
		zoo.close();
	}

	@Test
	public void testMultipleClients() throws Exception {
		MockZooConnector.connect(1);
		MockZooConnector.getZk(1).create("/test", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		MockZooConnector.connect(2);
		Assert.assertNotNull(MockZooConnector.getZk(2).exists("/test", false));
		MockZooConnector.close(1);
		MockZooConnector.close(2);
	}
}
