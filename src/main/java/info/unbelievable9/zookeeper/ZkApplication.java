package info.unbelievable9.zookeeper;

import java.io.IOException;

/**
 * ZooKeeper Demo 程序入口
 *
 * @author : Unbelievable9
 * @date : 2019-01-09
 */
public class ZkApplication {

    public static void main(String[] args) throws IOException, InterruptedException {

        // 创建基本会话
        //ZkWatcherSampleCallback.connect(properties);

        // 利用 Session 创建回话
        //ZkWatcherSampleCallback.connectWithSession(properties);

        // 同步创建节点
        //ZkWatcherSampleCallback.createZNodeSynchronously(properties);

        // 异步创建节点
        //ZkWatcherSampleCallback.createZNodeAsynchronously(properties);

        // 同步删除节点
        //ZkWatcherSampleCallback.createZNodeSynchronously(properties);
        //ZkWatcherSampleCallback.deleteZNodeSynchronously(properties);

        // 异步删除节点
        //ZkWatcherSampleCallback.createZNodeSynchronously(properties);
        //ZkWatcherSampleCallback.deleteZNodeAsynchronously(properties);

        // 同步读取子节点
        //ZkWatcherSampleCallback.getChildrenNodeSynchronously(properties);

        // 异步读取子节点
        //ZkWatcherSampleCallback.getChildrenNodeAsynchronously(properties);

        // 同步读取数据内容
        //ZkWatcherSampleCallback.getDataSynchronously(properties);

        // 异步读取数据内容
        //ZkWatcherSampleCallback.getDataAsynchronously(properties);
    }
}
