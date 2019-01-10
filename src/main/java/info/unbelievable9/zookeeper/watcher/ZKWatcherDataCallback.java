package info.unbelievable9.zookeeper.watcher;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;

/**
 * Author      : Unbelievable9
 * Class Name  :
 * Description :
 * Date        : 2019-01-10
 **/
public class ZKWatcherDataCallback implements AsyncCallback.DataCallback {

    @Override
    public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
        System.out.println("Get data result: " + new String(bytes));
        System.out.println(stat.getCzxid() + ", " + stat.getMzxid() + ", " + stat.getVersion());
    }
}
