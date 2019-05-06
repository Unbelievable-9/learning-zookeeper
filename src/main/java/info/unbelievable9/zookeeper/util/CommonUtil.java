package info.unbelievable9.zookeeper.util;

import info.unbelievable9.zookeeper.original.watcher.ZkWatcher;
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
                    5000,
                    new ZkWatcher());

            getConnectedSemaphore().await();

            return zooKeeper;
        }

        return null;
    }

    /**
     * 获取倒数计数器
     *
     * @return 倒数计数器对象
     */
    public static CountDownLatch getConnectedSemaphore() {
        return connectedSemaphore;
    }

    public static void refreshConnectedSemaphore() {
        connectedSemaphore = new CountDownLatch(1);
    }

    public static void setConnectedSemaphore(int count) {
        connectedSemaphore = new CountDownLatch(count);
    }
}
