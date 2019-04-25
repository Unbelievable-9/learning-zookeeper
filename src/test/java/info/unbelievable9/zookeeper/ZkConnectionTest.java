package info.unbelievable9.zookeeper;

import info.unbelievable9.zookeeper.util.CommonUtil;
import info.unbelievable9.zookeeper.watcher.ZkWatcherSampleCallback;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * 会话连接测试
 *
 * @author : unbelievable9
 * @date : 2019-04-24
 */
public class ZkConnectionTest extends ZkRootTest {

    private static final Logger logger = Logger.getLogger(ZkConnectionTest.class);

    private long sessionId = 0L;

    private byte[] sessionPasswd;

    /**
     * 创建简单回话测试用例
     *
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    @Test
    public void connect() throws IOException, InterruptedException {
        Assert.assertNotNull(properties);

        String connectString = properties.getProperty("zookeeper.server3.url")
                + ":"
                + properties.get("zookeeper.server3.port");

        CommonUtil.refreshConnectedSemaphore();

        ZooKeeper zooKeeper = new ZooKeeper(
                connectString,
                5000,
                new ZkWatcherSampleCallback());

        ZkWatcherSampleCallback.setZooKeeper(zooKeeper);

        logger.info("连接状态: " + zooKeeper.getState());

        sessionId = zooKeeper.getSessionId();
        sessionPasswd = zooKeeper.getSessionPasswd();

        CommonUtil.getConnectedSemaphore().await();

        Assert.assertEquals(zooKeeper.getState(), ZooKeeper.States.CONNECTED);
        zooKeeper.close();
        logger.info("连接已关闭");
    }

    /**
     * 利用 Session 信息复用回话
     *
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    @Test
    public void connectWithSession() throws IOException, InterruptedException {
        Assert.assertNotNull(properties);

        String connectString = properties.getProperty("zookeeper.server3.url")
                + ":"
                + properties.get("zookeeper.server3.port");

        CommonUtil.refreshConnectedSemaphore();

        // 使用错误的 Session 信息尝试连接
        ZooKeeper zooKeeper = new ZooKeeper(
                connectString,
                5000,
                new ZkWatcherSampleCallback(),
                1L,
                "test".getBytes());

        ZkWatcherSampleCallback.setZooKeeper(zooKeeper);

        logger.info("连接状态: " + zooKeeper.getState());

        CommonUtil.getConnectedSemaphore().await();

        Assert.assertEquals(zooKeeper.getState(), ZooKeeper.States.CLOSED);
        zooKeeper.close();
        logger.info("连接已关闭");

        CommonUtil.refreshConnectedSemaphore();

        // 使用正确的 Session 信息尝试连接
        zooKeeper = new ZooKeeper(
                connectString,
                5000,
                new ZkWatcherSampleCallback(),
                sessionId,
                sessionPasswd);

        ZkWatcherSampleCallback.setZooKeeper(zooKeeper);

        logger.info("连接状态: " + zooKeeper.getState());

        CommonUtil.getConnectedSemaphore().await();

        Assert.assertEquals(zooKeeper.getState(), ZooKeeper.States.CONNECTED);
        zooKeeper.close();
        logger.info("连接已关闭");
    }
}
