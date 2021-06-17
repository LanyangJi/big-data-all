package cn.jly.zk;

import org.apache.zookeeper.*;

import java.awt.*;
import java.nio.charset.StandardCharsets;
import java.rmi.registry.Registry;

/**
 * @author lanyangji
 * @date 2021/5/6 下午 1:33
 * @packageName cn.jly.zk
 * @className Zk05DistributeServer
 */
public class Zk05DistributeServer extends ZkConfig {

    private static final String PARENT_NODE = "/jly-servers";

    /**
     * 获取连接
     *
     * @return
     * @throws Exception
     */
    public ZooKeeper getConnect() throws Exception {
        return new ZooKeeper(ZK_SERVER_ADDR, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getType() + "--> " + event.getPath());
            }
        });
    }

    /**
     * 注册服务器
     *
     * @param zkClient
     * @param hostname
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void registerServer(ZooKeeper zkClient, String hostname) throws KeeperException, InterruptedException {
        String result = zkClient.create(PARENT_NODE + "/server", hostname.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " is online now: " + result);
    }

    /**
     * 业务功能
     *
     * @param hostname
     */
    public void business(String hostname) throws Exception {
        System.out.println(hostname + " is working.");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        Zk05DistributeServer server = new Zk05DistributeServer();
        ZooKeeper zkClient = server.getConnect();
        server.registerServer(zkClient, "node01");
        server.business("node01");
    }
}
