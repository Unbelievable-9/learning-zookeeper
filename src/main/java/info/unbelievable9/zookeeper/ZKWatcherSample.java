package info.unbelievable9.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Author      : Unbelievable9
 * Class Name  : ZKWatcherSample
 * Description : 通知接收 Watcher
 * Date        : 2019-01-09
 **/
public class ZKWatcherSample implements Watcher {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private static long sessionId = 0L;

    private static byte[] sessionPasswd;

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("收到通知: " + watchedEvent);

        if (watchedEvent.getState().equals(Event.KeeperState.SyncConnected)) {
            connectedSemaphore.countDown();

            System.out.println("ZooKeeper 已连接成功");
        } else {
            System.out.println("Zookeeper 连接失败: " + watchedEvent.getState().toString());
        }
    }

    static void connect(Properties properties) throws IOException, InterruptedException {
        // 建立会话
        String connectString = properties.getProperty("zookeeper.server1.url")
                + ":"
                + properties.get("zookeeper.server1.port");

        ZooKeeper zooKeeper = new ZooKeeper(
                connectString,
                5000,
                new ZKWatcherSample());

        System.out.println(zooKeeper.getState());

        sessionId = zooKeeper.getSessionId();
        sessionPasswd = zooKeeper.getSessionPasswd();

        ZKWatcherSample.connectedSemaphore.await();
    }

    static void connectWithSession(Properties properties) throws IOException, InterruptedException {
        String connectString = properties.getProperty("zookeeper.server1.url")
                + ":"
                + properties.get("zookeeper.server1.port");

        // 使用错误的 Session 信息尝试连接
        ZooKeeper zooKeeper = new ZooKeeper(
                connectString,
                5000,
                new ZKWatcherSample(),
                1L,
                "test".getBytes());

        System.out.println(zooKeeper.getState());

        // 使用正确的 Session 信息尝试连接
        zooKeeper = new ZooKeeper(
                connectString,
                5000,
                new ZKWatcherSample(),
                sessionId,
                sessionPasswd);

        System.out.println(zooKeeper.getState());

        Thread.sleep(Integer.MAX_VALUE);
    }
}

