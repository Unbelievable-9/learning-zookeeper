package info.unbelievable9.zookeeper;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Properties;

/**
 * Author      : Unbelievable9
 * Class Name  : ZKApplication
 * Description : ZooKeeper Demo 入口
 * Date        : 2019-01-09
 **/
public class ZKApplication {

    public static void main(String[] args) {
        // 从 classpath 读取配置文件
        String resourceName = "zookeeper.properties";
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Properties properties = new Properties();
        boolean loadConfigSuccess = true;

        try {
            properties.load(classLoader.getResourceAsStream(resourceName));
        } catch (IOException e) {
            loadConfigSuccess = false;

            e.printStackTrace();
            System.out.println("读取配置文件出错!");
        }

        if (loadConfigSuccess) {
            // 建立会话
            String connectString = properties.getProperty("zookeeper.server1.url")
                    + ":"
                    + properties.get("zookeeper.server1.port");

            try {
                ZooKeeper zooKeeper = new ZooKeeper(connectString, 5000, new ZKWatcherSample());

                System.out.println(zooKeeper.getState());

                ZKWatcherSample.connectedSemaphore.await();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("ZooKeeper 连接失败!");
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("阻塞失败");
            }
        }
    }
}
