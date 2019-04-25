package info.unbelievable9.zookeeper.watcher;

import info.unbelievable9.zookeeper.util.CommonUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;

/**
 * @author : unbelievable9
 * @date : 2019-01-09
 */
public class ZkWatcherStringCallback implements AsyncCallback.StringCallback {

    private static final Logger logger = Logger.getLogger(ZkWatcherStringCallback.class);

    @Override
    public void processResult(int i, String s, Object o, String s1) {
        if (i == 0) {
            CommonUtil.getConnectedSemaphore().countDown();
        }

        /*
          Result Code

          0    - OK
          -4   - ConnectionLoss 连接断开
          -110 - NodeExists     节点已存在
          -112 - SessionExpired 会话过期
         */
        logger.info(
                "Create znode result: [" +
                        "Result Code: " + i + ", " +
                        "Path: " + s + ", " +
                        "Context: " + o + ", " +
                        "Real Path: " + s1 + "]"
        );
    }
}
