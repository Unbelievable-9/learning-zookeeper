package info.unbelievable9.zookeeper;

import info.unbelievable9.zookeeper.util.CommonUtil;
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
import java.util.List;

/**
 * 节点同步操作测试
 *
 * @author : unbelievable9
 * @date : 2019-04-24
 */
@Test(groups = "node-operation-sync")
public class ZkNodeSyncOperationTest extends ZkRootTest {

    private static final Logger logger = Logger.getLogger(ZkNodeSyncOperationTest.class);

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
     * 同步创建节点
     *
     * @throws InterruptedException 中断异常
     */
    @Test(priority = 1)
    public void createZNodeSynchronously() throws InterruptedException {
        Assert.assertNotNull(zooKeeper);

        String firstPath = null;
        String secondPath = null;

        try {
            firstPath = zooKeeper.create(
                    "/sheep-znode",
                    "I'm a sheep with circle.".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            logger.error("同步创建圈羊节点失败!");

            e.printStackTrace();
        }

        if (firstPath != null) {
            logger.info("同步创建圈羊节点成功: " + firstPath);
        }

        try {
            secondPath = zooKeeper.create(
                    "/horse-znode",
                    "I'm a big horse.".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT
            );
        } catch (KeeperException e) {
            logger.error("同步创建大马节点失败!");

            e.printStackTrace();
        }

        if (secondPath != null) {
            logger.info("同步创建大马节点成功: " + secondPath);
        }
    }

    /**
     * 同步读取子节点
     *
     * @throws InterruptedException 中断异常
     */
    @Test(priority = 2)
    public void getChildrenNodeSynchronously() throws InterruptedException {
        Assert.assertNotNull(zooKeeper);

        String childPath;

        try {
            childPath = zooKeeper.create(
                    "/sheep-znode/baby-ship",
                    "Baby ship is here.".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

            if (childPath != null) {
                logger.info("同步创建羊崽子节点成功: " + childPath);
            }

            List<String> childrenList = zooKeeper.getChildren("/sheep-znode", true);

            logger.info("同步获得子节点列表: " + childrenList);
        } catch (KeeperException e) {
            logger.error("ZooKeeper 异常!");
            e.printStackTrace();
        }
    }

    /**
     * 同步删除节点
     *
     * @throws InterruptedException 中断异常
     */
    @Test(priority = 3)
    public void deleteZNodeSynchronously() throws InterruptedException {
        Assert.assertNotNull(zooKeeper);

        try {
            zooKeeper.delete("/sheep-znode/baby-ship", 0);
            logger.info("同步删除小圈羊节点成功");
        } catch (KeeperException e) {
            logger.error("ZooKeeper 出现异常!");
            e.printStackTrace();
        }

        try {
            zooKeeper.delete("/sheep-znode", 0);
            logger.info("同步删除圈羊节点成功");
        } catch (KeeperException e) {
            logger.error("ZooKeeper 出现异常!");
            e.printStackTrace();
        }

        try {
            zooKeeper.delete("/horse-znode", 0);
            logger.info("同步删除大马节点成功");
        } catch (KeeperException e) {
            logger.error("ZooKeeper 出现异常!");
            e.printStackTrace();
        }
    }
}
