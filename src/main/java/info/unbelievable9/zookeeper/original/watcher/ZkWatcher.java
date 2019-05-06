package info.unbelievable9.zookeeper.original.watcher;

import info.unbelievable9.zookeeper.util.CommonUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * 通知接收 Watcher
 *
 * @author : unbelievable9
 * @date : 2019-01-09
 */
public class ZkWatcher implements Watcher {

    private static final Logger logger = Logger.getLogger(ZkWatcher.class);

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState().equals(Event.KeeperState.SyncConnected)) {
            switch (watchedEvent.getType()) {
                case None:
                    if (watchedEvent.getPath() == null) {
                        CommonUtil.getConnectedSemaphore().countDown();

                        logger.info("Watcher 回调");
                    }

                    break;
                case NodeChildrenChanged:
                    logger.info("子节点 " + watchedEvent.getPath() + " 列表发生变化");

                    break;
                case NodeDataChanged:
                    logger.info("子节点" + watchedEvent.getPath() + " 信息发生变化");

                    break;
                default:
                    break;
            }
        } else {
            CommonUtil.getConnectedSemaphore().countDown();

            logger.error("ZooKeeper 连接失败: " + watchedEvent.getState().toString());
        }
    }
}