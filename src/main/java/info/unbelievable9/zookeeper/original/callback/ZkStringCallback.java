package info.unbelievable9.zookeeper.original.callback;

import info.unbelievable9.zookeeper.util.CommonUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;

/**
 * @author : unbelievable9
 * @date : 2019-01-09
 */
public class ZkStringCallback implements AsyncCallback.StringCallback {

    private static final Logger logger = Logger.getLogger(ZkStringCallback.class);

    @Override
    public void processResult(int i, String s, Object o, String s1) {
        if (i == 0) {
            logger.info(
                    "创建节点详情: [" +
                            "Result Code: " + i + ", " +
                            "Path: " + s + ", " +
                            "Context: " + o + ", " +
                            "Real Path: " + s1 + "]"
            );

            CommonUtil.getConnectedSemaphore().countDown();
        }
    }
}
