package info.unbelievable9.zookeeper.util;

import info.unbelievable9.zookeeper.original.watcher.ZkWatcher;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * 通用工具类
 *
 * @author : unbelievable9
 * @date : 2019-04-24
 */
public class CommonUtil {

    private static final Logger logger = Logger.getLogger(CommonUtil.class);

    private static CountDownLatch connectedSemaphore;

    /**
     * 获取计数器
     *
     * @return 倒数计数器对象
     */
    public static CountDownLatch getConnectedSemaphore() {
        return connectedSemaphore;
    }

    /**
     * 刷新计数器
     */
    public static void refreshConnectedSemaphore() {
        connectedSemaphore = new CountDownLatch(1);
    }

    /**
     * 设置计数器
     *
     * @param count 计数值
     */
    public static void setConnectedSemaphore(int count) {
        connectedSemaphore = new CountDownLatch(count);
    }

    /**
     * 获取 ZooKeeper配置文件
     *
     * @return 配置问文件对象
     */
    public static Properties getProperties() {
        String resourceName = "zookeeper.properties";
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Properties properties = new Properties();
        boolean loadConfigSuccess = true;

        try {
            properties.load(classLoader.getResourceAsStream(resourceName));
        } catch (IOException e) {
            loadConfigSuccess = false;

            e.printStackTrace();
            logger.info("读取配置文件出错!");
        }

        if (loadConfigSuccess) {
            return properties;
        } else {
            return null;
        }
    }

    /**
     * 获取 ZooKeeper 对象
     *
     * @return ZooKeeper 对象
     * @throws IOException          IO异常
     * @throws InterruptedException 中断异常
     */
    public static ZooKeeper getZooKeeper() throws IOException, InterruptedException {
        Properties properties = getProperties();

        if (properties != null) {
            refreshConnectedSemaphore();

            String connectString = properties.getProperty("zookeeper.server3.url")
                    + ":"
                    + properties.get("zookeeper.server3.port");

            ZooKeeper zooKeeper = new ZooKeeper(
                    connectString,
                    60000,
                    new ZkWatcher());

            getConnectedSemaphore().await();

            logger.info("会话已建立");

            return zooKeeper;
        }

        return null;
    }

    public static ZkClient getZkClient() {
        Properties properties = getProperties();

        if (properties != null) {
            String serverString = properties.getProperty("zookeeper.server3.url")
                    + ":"
                    + properties.get("zookeeper.server3.port");

            ZkClient zkClient = new ZkClient(serverString, 60000, 15000);

            logger.info("会话已建立");

            return zkClient;
        }

        return null;
    }

    public static CuratorFramework getCurator(String namespace) {
        Properties properties = getProperties();

        if (properties != null) {
            String connectString = properties.getProperty("zookeeper.server3.url")
                    + ":"
                    + properties.get("zookeeper.server3.port");

            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

            CuratorFramework client;

            if (namespace == null) {
                client = CuratorFrameworkFactory.builder()
                        .connectString(connectString)
                        .sessionTimeoutMs(5000)
                        .retryPolicy(retryPolicy)
                        .build();
            } else {
                client = CuratorFrameworkFactory.builder()
                        .connectString(connectString)
                        .sessionTimeoutMs(5000)
                        .retryPolicy(retryPolicy)
                        .namespace(namespace)
                        .build();
            }

            client.start();

            logger.info("会话已建立");

            return client;
        }

        return null;
    }
}
