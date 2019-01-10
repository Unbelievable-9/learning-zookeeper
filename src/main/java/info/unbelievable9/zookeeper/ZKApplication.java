package info.unbelievable9.zookeeper;

import java.io.IOException;
import java.util.Properties;

/**
 * Author      : Unbelievable9
 * Class Name  : ZKApplication
 * Description : ZooKeeper Demo 入口
 * Date        : 2019-01-09
 **/
public class ZKApplication {

    public static void main(String[] args) throws IOException, InterruptedException {

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
            // 创建基本会话
            //ZKWatcherSample.connect(properties);

            // 利用 Session 创建回话
            //ZKWatcherSample.connectWithSession(properties);

            // 同步创建节点
            //ZKWatcherSample.createZNodeSynchronously(properties);

            // 异步创建节点
            ZKWatcherSample.createZNodeAsynchronously(properties);
        }
    }
}
