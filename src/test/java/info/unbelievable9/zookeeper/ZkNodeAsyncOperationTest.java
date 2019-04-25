package info.unbelievable9.zookeeper;

import info.unbelievable9.zookeeper.util.CommonUtil;
import info.unbelievable9.zookeeper.watcher.ZkWatcherChildren2Callback;
import info.unbelievable9.zookeeper.watcher.ZkWatcherStringCallback;
import info.unbelievable9.zookeeper.watcher.ZkWatcherVoidCallback;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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
     */
    @Test(priority = 1)
    public void createZNodeAsynchronously() {
        Assert.assertNotNull(zooKeeper);

        CommonUtil.setConnectedSemaphore(4);

        zooKeeper.create(
                "/pig-znode",
                "I may be the first pig.".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZkWatcherStringCallback(),
                "My name is Peggy."
        );

        zooKeeper.create(
                "/duck-znode",
                "I may be the first duck".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZkWatcherStringCallback(),
                "My name is Donald."
        );

        zooKeeper.create(
                "/mouse-znode",
                "I may be the first mouse".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZkWatcherStringCallback(),
                "My name is Mickey."
        );

        logger.info("异步创建节点完成");
    }

    /**
     * 异步读取子节点
     *
     * @throws InterruptedException 中断异常
     */
    @Test(priority = 2)
    public void getChildrenNodeAsynchronously() throws InterruptedException, KeeperException {
        Assert.assertNotNull(zooKeeper);

        zooKeeper.create(
                "/pig-znode/baby-pig",
                "I may be the first baby pig.".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZkWatcherStringCallback(),
                "My name is Betty."
        );

        CommonUtil.getConnectedSemaphore().await();

        CommonUtil.refreshConnectedSemaphore();

        zooKeeper.getChildren("/pig-znode", true, new ZkWatcherChildren2Callback(), "异步获取 /pig-znode 下子节点列表");

        CommonUtil.getConnectedSemaphore().await();
    }

    /**
     * 异步删除节点
     */
    @Test(priority = 3)
    public void deleteZNodeAsynchronously() {
        Assert.assertNotNull(zooKeeper);

        zooKeeper.delete("/pig-znode/baby-pig", 0, new ZkWatcherVoidCallback(), "删除小 Peggy 节点");
        zooKeeper.delete("/pig-znode", 0, new ZkWatcherVoidCallback(), "删除 Peggy 节点");
        zooKeeper.delete("/duck-znode", 0, new ZkWatcherVoidCallback(), "删除 Donald 节点");
        zooKeeper.delete("/mouse-znode", 0, new ZkWatcherVoidCallback(), "删除 Mickey 节点");

        logger.info("异步删除节点完成");
    }
}
