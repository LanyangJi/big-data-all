package cn.jly.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lanyangji
 * @date 2021/5/6 下午 1:44
 * @packageName cn.jly.zk
 * @className Zk06DistributeClient
 */
public class Zk06DistributeClient extends ZkConfig {
    private ZooKeeper zkClient = null;
    private static final String PARENT_NODE = "/jly-servers";

    public void getConnect() throws Exception {
        Watcher watcher;
        this.zkClient = new ZooKeeper(ZK_SERVER_ADDR, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    // 获取服务器列表
                    getServerList();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 获取服务器列表
     */
    public void getServerList() throws KeeperException, InterruptedException {
        // 获取服务器子节点信息，并对父节点进行监听
        List<String> children = this.zkClient.getChildren(PARENT_NODE, true);
        // 存储服务器列表
        ArrayList<String> list = new ArrayList<>();
        // 遍历所有节点，获取主机名称
        for (String child : children) {
            byte[] data = this.zkClient.getData(PARENT_NODE + "/" + child, false, null);
            list.add(new String(data));
        }

        System.out.println("list = " + list);
    }

    public void business() throws Exception {
        System.out.println("client is working...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        Zk06DistributeClient client = new Zk06DistributeClient();
        client.getConnect();
        client.getServerList();
        client.business();
    }
}
