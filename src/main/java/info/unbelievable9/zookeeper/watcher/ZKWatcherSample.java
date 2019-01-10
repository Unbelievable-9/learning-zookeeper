package info.unbelievable9.zookeeper.watcher;

import org.apache.zookeeper.*;

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

            System.out.println("ZooKeeper 已连接");
        } else {
            System.out.println("Zookeeper 连接失败: " + watchedEvent.getState().toString());
        }
    }

    /**
     * 创建简单回话
     *
     * @param properties 配置信息
     * @return ZooKeeper实例
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    public static ZooKeeper connect(Properties properties) throws IOException, InterruptedException {
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

        return zooKeeper;
    }

    /**
     * 利用 Session 信息服用回话
     *
     * @param properties 配置信息
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    public static void connectWithSession(Properties properties) throws IOException, InterruptedException {
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

    /**
     * 同步方式创建节点
     *
     * @param properties 配置信息
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    public static void createZNodeSynchronously(Properties properties) throws IOException, InterruptedException {
        ZooKeeper zooKeeper = connect(properties);

        // 同步创建节点
        String firstPath = null;
        String secondPath = null;

        try {
            firstPath = zooKeeper.create(
                    "/sheep-znode",
                    "I'm a sheep with circle.".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            System.out.println("创建圈羊节点失败!");

            e.printStackTrace();
        }

        if (firstPath != null) {
            System.out.println("创建圈羊节点成功: " + firstPath);
        }

        try {
            secondPath = zooKeeper.create(
                    "/horse-znode",
                    "I'm a big horse.".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT
            );
        } catch (KeeperException e) {
            System.out.println("创建大马节点失败!");

            e.printStackTrace();
        }

        if (secondPath != null) {
            System.out.println("创建大马节点成功: " + secondPath);
        }
    }

    /**
     * 异步方式创建节点
     *
     * @param properties 配置信息
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    public static void createZNodeAsynchronously(Properties properties) throws IOException, InterruptedException {
        ZooKeeper zooKeeper = connect(properties);

        // 异步创建节点
        zooKeeper.create(
                "/pig-znode",
                "I may be the first pig.".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZKWatcherStringCallback(),
                "My name is Peggy."
        );

        zooKeeper.create(
                "/duck-znode",
                "I may be the first duck".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZKWatcherStringCallback(),
                "My name is Donald."
        );

        zooKeeper.create(
                "/mouse-znode",
                "I may be the first mouse".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZKWatcherStringCallback(),
                "My name is Mickey."
        );

        Thread.sleep(Integer.MAX_VALUE);
    }

    public static void deleteZNodeSynchronously(Properties properties) throws IOException, InterruptedException {
        ZooKeeper zooKeeper = connect(properties);

        // 同步删除节点
        try {
            zooKeeper.delete("/sheep-znode", 0);
        } catch (KeeperException e) {
            System.out.println("ZooKeeper 出现异常!");
            e.printStackTrace();
        }

        try {
            zooKeeper.delete("/horse-znode", 0);
        } catch (KeeperException e) {
            System.out.println("ZooKeeper 出现异常!");
            e.printStackTrace();
        }
    }

    public static void deleteZNodeAsynchronously(Properties properties) throws IOException, InterruptedException {
        ZooKeeper zooKeeper = connect(properties);

        // 异步删除节点
        zooKeeper.delete("/sheep-znode", 0, new ZKWacherVoidCallback(), "删除圈羊节点");
        zooKeeper.delete("/horse-znode", 0 , new ZKWacherVoidCallback(), "删除大马节点");

        Thread.sleep(Integer.MAX_VALUE);
    }
}

