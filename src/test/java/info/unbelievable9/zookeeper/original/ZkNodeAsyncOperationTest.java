package info.unbelievable9.zookeeper.original;

import info.unbelievable9.zookeeper.ZkRootTest;
import info.unbelievable9.zookeeper.original.callback.*;
import info.unbelievable9.zookeeper.util.CommonUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * 节点异步操作测试
 *
 * @author : unbelievable9
 * @date : 2019-04-24
 */
@Test(groups = "node-operation-async")
public class ZkNodeAsyncOperationTest extends ZkRootTest {

    private static final Logger logger = Logger.getLogger(ZkNodeAsyncOperationTest.class);

    private ZooKeeper zooKeeper;

    @BeforeTest
    @Override
    public void beforeTest() throws IOException, InterruptedException {
        super.beforeTest();

        zooKeeper = CommonUtil.getZookeeper();
    }

    @AfterTest
    @Override
    public void afterTest() throws InterruptedException {
        super.afterTest();

        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    /**
     * 异步创建节点
     *
     * @throws InterruptedException 中断异常
     */
    @Test(priority = 1)
    public void createZNodeAsynchronously() throws InterruptedException {
        Assert.assertNotNull(zooKeeper);

        CommonUtil.setConnectedSemaphore(4);

        logger.info("异步创建节点开始");

        zooKeeper.create(
                "/pig-znode",
                "I may be the first pig.".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZkStringCallback(),
                "My name is Peggy."
        );

        zooKeeper.create(
                "/duck-znode",
                "I may be the first duck".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZkStringCallback(),
                "My name is Donald."
        );

        zooKeeper.create(
                "/mouse-znode",
                "I may be the first mouse".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZkStringCallback(),
                "My name is Mickey."
        );

        zooKeeper.create(
                "/pig-znode/baby-pig",
                "I may be the first baby pig.".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZkStringCallback(),
                "My name is Betty."
        );

        CommonUtil.getConnectedSemaphore().await();

        logger.info("异步创建节点结束");
    }

    /**
     * 异步读取子节点列表信息
     *
     * @throws InterruptedException 中断异常
     */
    @Test(priority = 2)
    public void getChildrenNodeAsynchronously() throws InterruptedException {
        Assert.assertNotNull(zooKeeper);

        CommonUtil.refreshConnectedSemaphore();

        logger.info("异步读取子节点列表开始");

        zooKeeper.getChildren("/pig-znode", true, new ZkChildren2Callback(), "异步读取 /pig-znode 下子节点列表");

        CommonUtil.getConnectedSemaphore().await();

        logger.info("异步读取子节点列表结束");
    }

    /**
     * 异步读取并更新节点信息
     *
     * @throws InterruptedException 中断异常
     */
    @Test(priority = 3)
    public void getAndUpdateNodeAsynchronously() throws InterruptedException {
        Assert.assertNotNull(zooKeeper);

        CommonUtil.refreshConnectedSemaphore();

        logger.info("异步读取节点信息开始");

        zooKeeper.getData("/pig-znode", true, new ZkDataCallback(), "异步读取 /pig-znode 节点信息");

        CommonUtil.getConnectedSemaphore().await();

        logger.info("异步读取节点信息结束");

        logger.info("异步更新节点信息开始");

        CommonUtil.refreshConnectedSemaphore();

        zooKeeper.setData("/pig-znode", "Now my name is George.".getBytes(), -1, new ZkStatCallback(), "异步更新 /pig-znode 节点信息");

        CommonUtil.getConnectedSemaphore().await();

        logger.info("异步更新节点信息结束");

        CommonUtil.refreshConnectedSemaphore();

        logger.info("异步读取节点信息开始");

        zooKeeper.getData("/pig-znode", true, new ZkDataCallback(), "异步读取 /pig-znode 节点信息");

        CommonUtil.getConnectedSemaphore().await();

        logger.info("异步读取节点信息结束");
    }

    /**
     * 异步删除节点并检测节点是否删除成功
     *
     * @throws InterruptedException 中断异常
     */
    @Test(priority = 4)
    public void deleteZNodeAsynchronously() throws InterruptedException {
        Assert.assertNotNull(zooKeeper);

        CommonUtil.setConnectedSemaphore(4);

        logger.info("异步删除节点开始");

        zooKeeper.delete("/pig-znode/baby-pig", 0, new ZkVoidCallback(), "删除 /pig-znode/baby-pig 节点");
        zooKeeper.delete("/pig-znode", 1, new ZkVoidCallback(), "删除 /pig-znode 节点");
        zooKeeper.delete("/duck-znode", 0, new ZkVoidCallback(), "删除 /duck-znode 节点");
        zooKeeper.delete("/mouse-znode", 0, new ZkVoidCallback(), "删除 /mouse-znode 节点");

        CommonUtil.getConnectedSemaphore().await();

        logger.info("异步删除节点结束");

        CommonUtil.setConnectedSemaphore(4);

        logger.info("异步检测节点是否删除成功开始");

        zooKeeper.exists("/pig-znode/baby-pig", true, new ZkStatCallback(), "/pig-znode/baby-pig");
        zooKeeper.exists("/pig-znode", true, new ZkStatCallback(), "/pig-znode");
        zooKeeper.exists("/duck-znode", true, new ZkStatCallback(), "/duck-znode");
        zooKeeper.exists("/mouse-znode", true, new ZkStatCallback(), "/mouse-znode");

        CommonUtil.getConnectedSemaphore().await();

        logger.info("异步检测节点是否删除成功结束");
    }
}
