package info.unbelievable9.zookeeper.watcher;

import info.unbelievable9.zookeeper.util.CommonUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @author : unbelievable9
 * @date : 2019-01-09
 */
public class ZkWatcherChildren2Callback implements AsyncCallback.Children2Callback {

    private static final Logger logger = Logger.getLogger(ZkWatcherChildren2Callback.class);

    @Override
    public void processResult(int i, String s, Object o, List<String> list, Stat stat) {
        if (i == 0) {
            CommonUtil.getConnectedSemaphore().countDown();
        }

        logger.info(
                "Get children znode result: [" +
                        "Result Code: " + i + ", " +
                        "Path: " + s + ", " +
                        "Context: " + o + ", " +
                        "Children List: " + list + "]"
        );
    }
}
