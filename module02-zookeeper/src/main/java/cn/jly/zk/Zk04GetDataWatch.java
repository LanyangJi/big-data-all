package cn.jly.zk;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * @author lanyangji
 * @date 2021/5/6 上午 11:17
 * @packageName cn.jly.zk
 * @className Zk04GetDataWatch
 */
public class Zk04GetDataWatch extends ZkConfig {
    public static void main(String[] args) {
        try {
            ZooKeeper zkClient = new ZooKeeper(ZK_SERVER_ADDR, SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println(event.getType() + "--> " + event.getPath());
                }
            });

            // 监听数据变化
            zkClient.getData("/sanguo", true, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    System.out.println("data = " + new String(data));
                }
            }, null);

            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
