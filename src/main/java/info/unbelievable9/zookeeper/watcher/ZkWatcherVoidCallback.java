package info.unbelievable9.zookeeper.watcher;

import org.apache.zookeeper.AsyncCallback;

/**
 * @author : unbelievable9
 * @date : 2019-01-09
 */
public class ZkWatcherVoidCallback implements AsyncCallback.VoidCallback {

    @Override
    public void processResult(int i, String s, Object o) {
        System.out.println(
                "Delete znode result: [" +
                        "Result Code: " + i + ", " +
                        "Path: " + s + ", " +
                        "Context: " + o + "]"
        );
    }
}
