package info.unbelievable9.zookeeper.curator;

import info.unbelievable9.zookeeper.ZkRootTest;
import info.unbelievable9.zookeeper.util.CommonUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * 第三方客户端 Curator 测试
 *
 * @author : unbelievable9
 * @date : 2019-05-13
 */
public class CuratorSyncTest extends ZkRootTest {

    private static final Logger logger = Logger.getLogger(CuratorSyncTest.class);

    private static final String NAMESPACE = "curator";

    private static final String ADAM_PATH = "/adam";

    private static final String EVE_PATH = "/eva";

    private static final String SETH_PATH = "/eva/seth";


    private CuratorFramework client;

    @BeforeTest
    @Override
    public void beforeTest() throws IOException, InterruptedException {
        super.beforeTest();

        client = CommonUtil.getCurator(NAMESPACE);

        Assert.assertNotNull(client);
    }

    @AfterTest
    @Override
    public void afterTest() throws InterruptedException {
        super.afterTest();

        if (client != null && !client.getState().equals(CuratorFrameworkState.STOPPED)) {
            client.close();
        }
    }

    @Test(priority = 1)
    public void createNodeTest() {
        try {
            // ZooKeeper 规定所有非叶子节点必须为持久节点 所以 /curator 非叶子节点会被保留
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(ADAM_PATH, "I am the first man.".getBytes());
        } catch (Exception e) {
            logger.error("创建节点失败");
        }

        try {
            Assert.assertNotNull(client.checkExists().forPath(ADAM_PATH));
        } catch (Exception e) {
            logger.error("创建节点失败");
        }

        logger.info("创建节点成功");
    }

    @Test(priority = 2)
    public void getNodeDataTest() {
        Stat stat = new Stat();

        try {
            byte[] data = client.getData()
                    .storingStatIn(stat)
                    .forPath(ADAM_PATH);

            logger.info("节点信息: " + new String(data));
        } catch (Exception e) {
            logger.error("获取节点信息失败");
        }
    }

    @Test(priority = 3)
    public void updateNodeDataTest() {
        Stat stat = new Stat();

        try {
            client.setData()
                    .forPath(ADAM_PATH, "I am son of God.".getBytes());

            byte[] data = client.getData()
                    .storingStatIn(stat)
                    .forPath(ADAM_PATH);

            logger.info("第一次更新节点信息成功，节点信息: " + new String(data));

            client.setData()
                    .withVersion(stat.getVersion())
                    .forPath(ADAM_PATH, "I ate the fruit of knowledge.".getBytes());

            data = client.getData()
                    .storingStatIn(stat)
                    .forPath(ADAM_PATH);

            logger.info("第二次更新节点信息成功，节点信息: " + new String(data));
        } catch (Exception e) {
            logger.error("更新节点信息失败");
        }
    }

    @Test(priority = 4)
    public void deleteNodeTest() {
        try {
            client.create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(EVE_PATH, "I am the first woman.".getBytes());

            client.create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(SETH_PATH, "I am son of Eve.".getBytes());
        } catch (Exception e) {
            logger.error("创建节点失败");
        }

        try {
            client.delete()
                    .deletingChildrenIfNeeded()
                    .withVersion(-1)
                    .forPath(EVE_PATH);

            logger.info("删除节点成功");
        } catch (Exception e) {
            logger.error("删除节点失败");
        }
    }
}
