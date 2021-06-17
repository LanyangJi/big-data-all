package cn.jly.zk;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author lanyangji
 * @date 2021/5/6 上午 11:07
 * @packageName cn.jly.zk
 * @className Zk02CreateNode
 */
public class Zk02CreateNode extends ZkConfig{
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zkClient = new ZooKeeper(ZK_SERVER_ADDR, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "--> " + watchedEvent.getPath());
            }
        });

        String result = zkClient.create("/test", "tom".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("result = " + result);
    }
}
