package cn.jly.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;

/**
 * @author lanyangji
 * @date 2021/4/20 上午 11:00
 * @packageName cn.jly.hadoop.hdfs
 * @className Hdfs06FileDetail
 */
public class Hdfs06FileDetail extends BaseConfig {
    public static void main(String[] args) {
        init();

        Configuration configuration = new Configuration();

        try (FileSystem fs = FileSystem.get(new URI(HDFS_URI), configuration, USER)) {
            // 查看文件详情
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
            while (listFiles.hasNext()){
                LocatedFileStatus locatedFileStatus = listFiles.next();
                System.out.println("fileName = " + locatedFileStatus.getPath().getName());
                System.out.println("length = " + locatedFileStatus.getLen());
                System.out.println("permission = " + locatedFileStatus.getPermission());
                System.out.println("group = " + locatedFileStatus.getGroup());

                // 块信息
                BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
                for (BlockLocation blockLocation : blockLocations) {
                    // 主机
                    String[] hosts = blockLocation.getHosts();
                    for (String host : hosts) {
                        System.out.println("host = " + host);
                    }
                }
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
