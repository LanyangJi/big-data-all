package cn.jly.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * @author lanyangji
 * @date 2021/5/6 上午 10:52
 * @packageName cn.jly.zk
 * @className Zk01Connect
 */
public class Zk01Connect extends ZkConfig {
    public static void main(String[] args) throws IOException {
        // 创建zk客户端
        ZooKeeper zkClient = new ZooKeeper(ZK_SERVER_ADDR, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 监听回调逻辑
                System.out.println(watchedEvent.getType() + "--> " + watchedEvent.getPath());
            }
        });
    }
}
