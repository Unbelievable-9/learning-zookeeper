package info.unbelievable9.zookeeper.curator;

import info.unbelievable9.zookeeper.ZkRootTest;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * 第三方客户端 Curator 测试
 *
 * @author : unbelievable9
 * @date : 2019-05-13
 */
public class CuratorTest extends ZkRootTest {

    private static final Logger logger = Logger.getLogger(CuratorTest.class);

    private CuratorFramework client;

    /**
     * 简单会话建立测试
     */
    @Test(priority = 1)
    public void sampleSessionTest() {
        Assert.assertNotNull(properties);

        String connectString = properties.getProperty("zookeeper.server3.url")
                + ":"
                + properties.get("zookeeper.server3.port");

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        client = CuratorFrameworkFactory.newClient(connectString, 5000, 300, retryPolicy);
        client.start();

        Assert.assertEquals(client.getState(), CuratorFrameworkState.STARTED);

        logger.info("会话建立已开始");
    }
}
