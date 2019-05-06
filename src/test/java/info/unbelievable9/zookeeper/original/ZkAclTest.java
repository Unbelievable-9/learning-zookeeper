package info.unbelievable9.zookeeper.original;

import info.unbelievable9.zookeeper.ZkRootTest;
import info.unbelievable9.zookeeper.util.CommonUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * ACL 权限控制测试
 *
 * @author : unbelievable9
 * @date : 2019-05-06
 */
public class ZkAclTest extends ZkRootTest {

    private static final Logger logger = Logger.getLogger(ZkAclTest.class);

    private static final String EDEN_PATH = "/eden";

    private static final String SOPHIA_PATH = "/eden/sophia";

    private ZooKeeper zooKeeperOne, zooKeeperTwo, zooKeeperThree, zooKeeperFour;

    @BeforeTest
    @Override
    public void beforeTest() throws IOException, InterruptedException {
        super.beforeTest();

        zooKeeperOne = CommonUtil.getZooKeeper();
        zooKeeperTwo = CommonUtil.getZooKeeper();
        zooKeeperThree = CommonUtil.getZooKeeper();
        zooKeeperFour = CommonUtil.getZooKeeper();
    }

    @AfterTest
    @Override
    public void afterTest() throws InterruptedException {
        super.afterTest();

        if (zooKeeperOne != null) {
            zooKeeperOne.close();
        }

        if (zooKeeperTwo != null) {
            zooKeeperTwo.close();
        }

        if (zooKeeperThree != null) {
            zooKeeperThree.close();
        }

        if (zooKeeperFour != null) {
            zooKeeperFour.close();
        }
    }

    @Test(priority = 1)
    public void sampleAuthTest() {
        zooKeeperOne.addAuthInfo("digest", "jack:cool".getBytes());

        try {
            zooKeeperOne.create(EDEN_PATH, "Nightside of Eden.".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);
            zooKeeperTwo.getData(EDEN_PATH, false, null);
        } catch (KeeperException | InterruptedException e) {
            Assert.assertEquals(e.getClass(), NoAuthException.class);

            logger.info("ZooKeeperTwo 没有权限访问节点");

            try {
                zooKeeperOne.delete(EDEN_PATH, -1);
            } catch (InterruptedException | KeeperException ex) {
                ex.printStackTrace();
            }
        }
    }

    @Test(priority = 2)
    public void authTest() {
        try {
            zooKeeperOne.create(EDEN_PATH, "Nightside of Eden".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);

            zooKeeperTwo.addAuthInfo("digest", "jack:cool".getBytes());

            byte[] data = zooKeeperTwo.getData(EDEN_PATH, false, null);

            logger.info("节点 " + EDEN_PATH + " 信息: " + new String(data));

            zooKeeperThree.addAuthInfo("digest", "jack:sucks".getBytes());

            data = zooKeeperThree.getData(EDEN_PATH, false, null);

            logger.info("节点 " + EDEN_PATH + " 信息: " + new String(data));
        } catch (KeeperException | InterruptedException e) {
            Assert.assertEquals(e.getClass(), NoAuthException.class);

            logger.info("ZooKeeperThree 没有权限访问节点");

            try {
                zooKeeperTwo.delete(EDEN_PATH, -1);
            } catch (InterruptedException | KeeperException ex) {
                ex.printStackTrace();
            }
        }
    }

    @Test(priority = 3)
    public void deleteAuthTest() {
        try {
            zooKeeperOne.create(EDEN_PATH, "Nightside of Eden".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            zooKeeperOne.create(SOPHIA_PATH, "The Perennial Sophia".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);

            zooKeeperThree.delete(SOPHIA_PATH, -1);
        } catch (KeeperException | InterruptedException e) {
            Assert.assertEquals(e.getClass(), NoAuthException.class);

            logger.info("ZooKeeperThree 没有权限删除节点");
        }

        try {
            zooKeeperTwo.delete(SOPHIA_PATH, -1);

            logger.info("ZooKeeperThree 成功删除节点: " + SOPHIA_PATH);

            // ZooKeeperFour 并没有权限 但是因为子节点 /sophia 已经被有权限的 ZookeeperTwo 删除 所以 /eden 可被删除 (ZooKeeper 的为什么这么设计?)
            zooKeeperFour.delete(EDEN_PATH, -1);

            logger.info("ZooKeeperFour 成功删除节点: " + EDEN_PATH);
        } catch (InterruptedException | KeeperException e) {
            logger.error("ZooKeeper 异常！");

            e.printStackTrace();
        }
    }
}
