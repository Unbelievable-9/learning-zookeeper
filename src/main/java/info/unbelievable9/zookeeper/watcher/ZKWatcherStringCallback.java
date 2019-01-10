package info.unbelievable9.zookeeper.watcher;

import org.apache.zookeeper.AsyncCallback;

/**
 * Author      : Unbelievable9
 * Class Name  : 节点创建回调
 * Description : ZKWatcherStringCallback
 * Date        : 2019-01-10
 **/
public class ZKWatcherStringCallback implements AsyncCallback.StringCallback {

    @Override
    public void processResult(int i, String s, Object o, String s1) {
        /*
          Result Code

          0    - OK
          -4   - ConnectionLoss 连接断开
          -110 - NodeExists     节点已存在
          -112 - SessionExpired 会话过期
         */
        System.out.println(
                "Process Result: [" +
                        "Result Code: " + i + ", " +
                        "Path: " + s + ", " +
                        "Context: " + o + ", " +
                        "Real Path: " + s1 + "]"
        );
    }
}
