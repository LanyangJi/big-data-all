package cn.jly.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

/**
 * @author lanyangji
 * @date 2021/5/6 上午 11:13
 * @packageName cn.jly.zk
 * @className Zk03GetChildrenWatch
 */
public class Zk03GetChildrenWatch extends ZkConfig {
    public static void main(String[] args) {
        try {
            ZooKeeper zkClient = new ZooKeeper(ZK_SERVER_ADDR, SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println(event.getType() + "--> " + event.getPath());
                }
            });

            List<String> children = zkClient.getChildren("/", true);
            for (String child : children) {
                System.out.println("child = " + child);
            }

            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
