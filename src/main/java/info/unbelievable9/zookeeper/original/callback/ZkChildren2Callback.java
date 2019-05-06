package info.unbelievable9.zookeeper.original.callback;

import info.unbelievable9.zookeeper.util.CommonUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @author : unbelievable9
 * @date : 2019-01-09
 */
public class ZkChildren2Callback implements AsyncCallback.Children2Callback {

    private static final Logger logger = Logger.getLogger(ZkChildren2Callback.class);

    @Override
    public void processResult(int i, String s, Object o, List<String> list, Stat stat) {
        if (i == 0) {
            logger.info(
                    "异步获取子节点列表详情: [" +
                            "Result Code: " + i + ", " +
                            "Path: " + s + ", " +
                            "Context: " + o + ", " +
                            "Children List: " + list + "]"
            );

            CommonUtil.getConnectedSemaphore().countDown();
        }
    }
}
