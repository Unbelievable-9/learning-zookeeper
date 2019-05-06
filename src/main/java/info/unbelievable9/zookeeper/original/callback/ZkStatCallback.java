package info.unbelievable9.zookeeper.original.callback;

import info.unbelievable9.zookeeper.util.CommonUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;

/**
 * @author : unbelievable9
 * @date : 2019-05-06
 */
public class ZkStatCallback implements AsyncCallback.StatCallback {

    private static final Logger logger = Logger.getLogger(ZkStatCallback.class);

    @Override
    public void processResult(int i, String s, Object o, Stat stat) {
        if (i == 0) {
            logger.info("节点详情: [" +
                    "czxid:" + stat.getCzxid() + ", " +
                    "mzxid:" + stat.getMzxid() + ", " +
                    "version:" + stat.getVersion() + "]");

            CommonUtil.getConnectedSemaphore().countDown();
        }
    }
}
