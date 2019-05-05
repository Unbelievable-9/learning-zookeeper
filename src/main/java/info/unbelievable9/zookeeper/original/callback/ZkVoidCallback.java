package info.unbelievable9.zookeeper.original.callback;

import info.unbelievable9.zookeeper.util.CommonUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;

/**
 * @author : unbelievable9
 * @date : 2019-01-09
 */
public class ZkVoidCallback implements AsyncCallback.VoidCallback {

    private static final Logger logger = Logger.getLogger(ZkVoidCallback.class);

    @Override
    public void processResult(int i, String s, Object o) {
        if (i == 0) {
            logger.info(
                    "删除节点详情: [" +
                            "Result Code: " + i + ", " +
                            "Path: " + s + ", " +
                            "Context: " + o + "]"
            );

            CommonUtil.getConnectedSemaphore().countDown();
        }
    }
}
