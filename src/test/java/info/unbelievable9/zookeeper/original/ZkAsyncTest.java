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
public class ZkAsyncTest extends ZkRootTest {

    private static final Logger logger = Logger.getLogger(ZkAsyncTest.class);

    private static final String PIG_PATH = "/pig-znode";

    private static final String DUCK_PATH = "/duck-znode";

    private static final String MOUSE_PATH = "/mouse-znode";

    private static final String BABY_PIG_PATH = "/pig-znode/bay-pig-znode";

    private ZooKeeper zooKeeper;

    @BeforeTest
    @Override
    public void beforeTest() throws IOException, InterruptedException {
        super.beforeTest();

        zooKeeper = CommonUtil.getZooKeeper();

        Assert.assertNotNull(zooKeeper);
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
    public void createNodeTest() throws InterruptedException {
        CommonUtil.setConnectedSemaphore(4);

        logger.info("异步创建节点开始");

        zooKeeper.create(
                PIG_PATH,
                "I may be the first pig.".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZkStringCallback(),
                "My name is Peggy."
        );

        zooKeeper.create(
                DUCK_PATH,
                "I may be the first duck".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZkStringCallback(),
                "My name is Donald."
        );

        zooKeeper.create(
                MOUSE_PATH,
                "I may be the first mouse".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZkStringCallback(),
                "My name is Mickey."
        );

        zooKeeper.create(
                BABY_PIG_PATH,
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
    public void getChildNodeTest() throws InterruptedException {
        CommonUtil.refreshConnectedSemaphore();

        logger.info("异步读取子节点列表开始");

        zooKeeper.getChildren(PIG_PATH, true, new ZkChildren2Callback(), "异步读取 " + PIG_PATH + " 下子节点列表");

        CommonUtil.getConnectedSemaphore().await();

        logger.info("异步读取子节点列表结束");
    }

    /**
     * 异步读取并更新节点信息
     *
     * @throws InterruptedException 中断异常
     */
    @Test(priority = 3)
    public void updateNodeTest() throws InterruptedException {
        CommonUtil.refreshConnectedSemaphore();

        logger.info("异步读取节点信息开始");

        zooKeeper.getData(PIG_PATH, true, new ZkDataCallback(), "异步读取 " + PIG_PATH + " 节点信息");

        CommonUtil.getConnectedSemaphore().await();

        logger.info("异步读取节点信息结束");

        logger.info("异步更新节点信息开始");

        CommonUtil.refreshConnectedSemaphore();

        zooKeeper.setData(PIG_PATH, "Now my name is George.".getBytes(), -1, new ZkStatCallback(), "异步更新 " + PIG_PATH + " 节点信息");

        CommonUtil.getConnectedSemaphore().await();

        logger.info("异步更新节点信息结束");

        CommonUtil.refreshConnectedSemaphore();

        logger.info("异步读取节点信息开始");

        zooKeeper.getData(PIG_PATH, true, new ZkDataCallback(), "异步读取 " + PIG_PATH + " 节点信息");

        CommonUtil.getConnectedSemaphore().await();

        logger.info("异步读取节点信息结束");
    }

    /**
     * 异步删除节点并检测节点是否删除成功
     *
     * @throws InterruptedException 中断异常
     */
    @Test(priority = 4)
    public void deleteNodeTest() throws InterruptedException {
        CommonUtil.setConnectedSemaphore(4);

        logger.info("异步删除节点开始");

        zooKeeper.delete(BABY_PIG_PATH, 0, new ZkVoidCallback(), "删除 " + BABY_PIG_PATH + " 节点");
        zooKeeper.delete(PIG_PATH, 1, new ZkVoidCallback(), "删除 " + PIG_PATH + " 节点");
        zooKeeper.delete(DUCK_PATH, 0, new ZkVoidCallback(), "删除 " + DUCK_PATH + " 节点");
        zooKeeper.delete(MOUSE_PATH, 0, new ZkVoidCallback(), "删除 " + MOUSE_PATH + " 节点");

        CommonUtil.getConnectedSemaphore().await();

        logger.info("异步删除节点结束");

        CommonUtil.setConnectedSemaphore(4);

        logger.info("异步检测节点是否删除成功开始");

        zooKeeper.exists(BABY_PIG_PATH, true, new ZkStatCallback(), BABY_PIG_PATH);
        zooKeeper.exists(PIG_PATH, true, new ZkStatCallback(), PIG_PATH);
        zooKeeper.exists(DUCK_PATH, true, new ZkStatCallback(), DUCK_PATH);
        zooKeeper.exists(MOUSE_PATH, true, new ZkStatCallback(), MOUSE_PATH);

        CommonUtil.getConnectedSemaphore().await();

        logger.info("异步检测节点是否删除成功结束");
    }
}
