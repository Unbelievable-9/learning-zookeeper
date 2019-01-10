package info.unbelievable9.zookeeper.watcher;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
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

    private static ZooKeeper zooKeeper;

    private static Stat stat = new Stat();

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("收到通知: " + watchedEvent);

        if (watchedEvent.getState().equals(Event.KeeperState.SyncConnected)) {
            System.out.println("ZooKeeper 已连接");

            switch (watchedEvent.getType()) {
                case None:
                    if (watchedEvent.getPath() == null) {
                        connectedSemaphore.countDown();
                    }
                    break;
                case NodeChildrenChanged:
                    // 子节点发生变化, 重新获取子节点
                    try {
                        List<String> childrenList = zooKeeper.getChildren(watchedEvent.getPath(), true);

                        System.out.println("Re-Get children: " + childrenList);
                    } catch (KeeperException | InterruptedException e) {
                        System.out.println("ZooKeeper 异常!");
                        e.printStackTrace();
                    }
                    break;
                case NodeDataChanged:
                    // 数据内容发生变化, 重新获取数据内容
                    try {
                        String data = new String(zooKeeper.getData(watchedEvent.getPath(), true, stat));

                        System.out.println("Re-Get data: " + data);
                        System.out.println(stat.getCzxid() + ", " + stat.getMzxid() + ", " + stat.getVersion());
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                    break;
            }
        } else {
            System.out.println("Zookeeper 连接失败: " + watchedEvent.getState().toString());
        }
    }

    /**
     * 同步添加测试节点
     *
     * @param properties 配置信息
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    private static void createDemoNode(Properties properties) throws IOException, InterruptedException {
        zooKeeper = connect(properties);

        // 同步创建测试节点
        try {
            zooKeeper.create(
                    "/sheep-znode",
                    "All sheep are here.".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT
            );

            zooKeeper.create(
                    "/sheep-znode/baby-sheep",
                    "Baby sheep are here.".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL
            );
        } catch (KeeperException e) {
            System.out.println("ZooKeeper 异常!");
            e.printStackTrace();
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

    /**
     * 同步删除节点
     *
     * @param properties 配置信息
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
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

    /**
     * 异步删除节点
     *
     * @param properties 配置信息
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    public static void deleteZNodeAsynchronously(Properties properties) throws IOException, InterruptedException {
        ZooKeeper zooKeeper = connect(properties);

        // 异步删除节点
        zooKeeper.delete("/sheep-znode", 0, new ZKWatcherVoidCallback(), "删除圈羊节点");
        zooKeeper.delete("/horse-znode", 0, new ZKWatcherVoidCallback(), "删除大马节点");

        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 同步读取子节点
     *
     * @param properties 配置信息
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    public static void getChildrenNodeSynchronously(Properties properties) throws IOException, InterruptedException {
        createDemoNode(properties);

        // 同步读取子节点
        try {
            List<String> childrenList = zooKeeper.getChildren("/sheep-znode", true);

            System.out.println("Get children: " + childrenList);
        } catch (KeeperException e) {
            System.out.println("ZooKeeper 异常!");
            e.printStackTrace();
        }

        // 可以手动去 ZooKeeper 里在 /sheep-znode 下创建新的子节点看效果
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 异步读取子节点
     *
     * @param properties 配置信息
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    public static void getChildrenNodeAsynchronously(Properties properties) throws IOException, InterruptedException {
        createDemoNode(properties);

        // 异步获取子节点
        zooKeeper.getChildren("/sheep-znode", true, new ZKWatcherChildren2Callback(), "Get children node in /sheep-znode.");

        // 可以手动去 ZooKeeper 里在 /sheep-znode 下创建新的子节点看效果
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 同步读取数据内容
     *
     * @param properties 配置信息
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    public static void getDataSynchronously(Properties properties) throws IOException, InterruptedException {
        createDemoNode(properties);

        try {
            String data = new String(zooKeeper.getData("/sheep-znode/baby-sheep", true, stat));

            System.out.println("Get data: " + data);
            System.out.print(stat.getCzxid() + ", " + stat.getMzxid() + ", " + stat.getVersion());
        } catch (KeeperException e) {
            System.out.println("Zookeeper 异常!");
            e.printStackTrace();
        }

        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 异步读取数据内容
     *
     * @param properties 配置信息
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    public static void getDataAsynchronously(Properties properties) throws IOException, InterruptedException {
        createDemoNode(properties);

        zooKeeper.getData("/sheep-znode/baby-sheep", true, new ZKWatcherDataCallback(), null);

        Thread.sleep(Integer.MAX_VALUE);
    }
}

