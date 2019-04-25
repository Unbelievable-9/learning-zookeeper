package info.unbelievable9.zookeeper.watcher;

import info.unbelievable9.zookeeper.util.CommonUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * 通知接收 Watcher
 *
 * @author : unbelievable9
 * @date : 2019-01-09
 */
public class ZkWatcherSampleCallback implements Watcher {

    private static final Logger logger = Logger.getLogger(ZkWatcherSampleCallback.class);

    private static ZooKeeper zooKeeper;

    private static Stat stat = new Stat();

    public static void setZooKeeper(ZooKeeper zooKeeper) {
        ZkWatcherSampleCallback.zooKeeper = zooKeeper;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info("收到通知: " + watchedEvent);

        if (watchedEvent.getState().equals(Event.KeeperState.SyncConnected)) {
            logger.info("ZooKeeper 已连接");

            switch (watchedEvent.getType()) {
                case None:
                    if (watchedEvent.getPath() == null) {
                        CommonUtil.getConnectedSemaphore().countDown();
                    }
                    break;
                case NodeChildrenChanged:
                    // 子节点发生变化, 重新获取子节点
                    try {
                        List<String> childrenList = zooKeeper.getChildren(watchedEvent.getPath(), true);

                        logger.info("Re-Get children: " + childrenList);
                    } catch (KeeperException | InterruptedException e) {
                        logger.info("ZooKeeper 异常!");
                        e.printStackTrace();
                    }
                    break;
                case NodeDataChanged:
                    // 数据内容发生变化, 重新获取数据内容
                    try {
                        String data = new String(zooKeeper.getData(watchedEvent.getPath(), true, stat));

                        logger.info("Re-Get data: " + data);
                        logger.info(stat.getCzxid() + ", " + stat.getMzxid() + ", " + stat.getVersion());
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                default:
                    break;
            }
        } else {
            CommonUtil.getConnectedSemaphore().countDown();

            logger.info("Zookeeper 连接失败: " + watchedEvent.getState().toString());
        }
    }
}

