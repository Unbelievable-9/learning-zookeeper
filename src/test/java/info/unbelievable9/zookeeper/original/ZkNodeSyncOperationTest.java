package info.unbelievable9.zookeeper.original;

import info.unbelievable9.zookeeper.ZkRootTest;
import info.unbelievable9.zookeeper.util.CommonUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
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
     */
    @Test(priority = 1)
    public void createZNodeSynchronously() {
        Assert.assertNotNull(zooKeeper);

        String firstPath = null;
        String secondPath = null;
        String childPath = null;

        try {
            firstPath = zooKeeper.create(
                    "/sheep-znode",
                    "I'm a sheep with circle.".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
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
        } catch (KeeperException | InterruptedException e) {
            logger.error("同步创建大马节点失败!");

            e.printStackTrace();
        }

        if (secondPath != null) {
            logger.info("同步创建大马节点成功: " + secondPath);
        }

        try {
            childPath = zooKeeper.create(
                    "/sheep-znode/baby-ship",
                    "Baby ship is here.".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            logger.error("同步创建小圈羊节点失败!");

            e.printStackTrace();
        }

        if (childPath != null) {
            logger.info("同步创建小圈羊节点成功: " + childPath);
        }
    }

    /**
     * 同步读取子节点列表信息
     */
    @Test(priority = 2)
    public void getChildrenNodeSynchronously() {
        Assert.assertNotNull(zooKeeper);

        try {
            List<String> childrenList = zooKeeper.getChildren("/sheep-znode", true);
            logger.info("同步获得子节点列表: " + childrenList);
        } catch (KeeperException | InterruptedException e) {
            logger.error("ZooKeeper 异常!");
            e.printStackTrace();
        }
    }

    /**
     * 同步读取并更新节点信息
     */
    @Test(priority = 3)
    public void getAndUpdateNodeDataSynchronously() {
        Assert.assertNotNull(zooKeeper);

        try {
            Stat stat = new Stat();
            byte[] data = zooKeeper.getData("/sheep-znode", true, stat);

            logger.info("节点信息: " +  new String(data));
            logger.info("节点详情: [" +
                    "czxid:" + stat.getCzxid() + ", " +
                    "mzxid:" + stat.getMzxid() + ", " +
                    "version:" + stat.getVersion() + "]");

            // 无原子性要求时，version 使用 -1 可对数据最新版本进行更新
            stat = zooKeeper.setData("/sheep-znode", "I'm now Jack Zhao.".getBytes(), -1);

            logger.info("更新后节点详情: [" +
                    "czxid:" + stat.getCzxid() + ", " +
                    "mzxid:" + stat.getMzxid() + ", " +
                    "version:" + stat.getVersion() + "]");

            stat = new Stat();
            data = zooKeeper.getData("/sheep-znode", true, stat);

            logger.info("节点信息: " +  new String(data));
            logger.info("节点详情: [" +
                    "czxid:" + stat.getCzxid() + ", " +
                    "mzxid:" + stat.getMzxid() + ", " +
                    "version:" + stat.getVersion() + "]");
        } catch (InterruptedException | KeeperException e) {
            logger.error("Zookeeper 异常!");
            e.printStackTrace();
        }
    }

    /**
     * 同步删除节点并检测节点是否删除成功
     */
    @Test(priority = 4)
    public void deleteZNodeSynchronously() {
        Assert.assertNotNull(zooKeeper);

        Stat stat;

        try {
            zooKeeper.delete("/sheep-znode/baby-ship", 0);

            stat = zooKeeper.exists("/sheep-znode/baby-ship", true);

            if (stat == null) {
                logger.info("同步删除小圈羊节点成功");
            } else {
                logger.warn("同步删除小圈羊节点失败");
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("ZooKeeper 异常!");
            e.printStackTrace();
        }

        try {
            zooKeeper.delete("/sheep-znode", 1);

            stat = zooKeeper.exists("/sheep-znode", true);

            if (stat == null) {
                logger.info("同步删除圈羊节点成功");
            } else {
                logger.error("同步删除圈羊节点失败");
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("ZooKeeper 异常!");
            e.printStackTrace();
        }

        try {
            zooKeeper.delete("/horse-znode", 0);

            stat = zooKeeper.exists("/horse-znode", true);

            if (stat == null) {
                logger.info("同步删除大马节点成功");
            } else {
                logger.error("同步删除大马节点失败");
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error("ZooKeeper 异常!");
            e.printStackTrace();
        }
    }
}
