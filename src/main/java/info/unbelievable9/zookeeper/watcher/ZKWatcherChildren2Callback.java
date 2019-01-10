package info.unbelievable9.zookeeper.watcher;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * Author      : Unbelievable9
 * Class Name  :
 * Description :
 * Date        : 2019-01-10
 **/
public class ZKWatcherChildren2Callback implements AsyncCallback.Children2Callback {

    @Override
    public void processResult(int i, String s, Object o, List<String> list, Stat stat) {
        System.out.println(
                "Get children znode result: [" +
                        "Result Code: " + i + ", " +
                        "Path: " + s + ", " +
                        "Context: " + o + ", " +
                        "Children List: " + list + "]"
        );
    }
}
