package cn.jly.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jboss.netty.channel.ExceptionEvent;

import java.net.URI;

import static cn.jly.hadoop.hdfs.BaseConfig.init;

/**
 * 删除
 *
 * @author lanyangji
 * @date 2021/4/20 上午 10:47
 * @packageName cn.jly.hadoop.hdfs
 * @className Hdfs04Delete
 */
public class Hdfs04Delete extends BaseConfig {
    public static void main(String[] args) {
        init();

        Configuration configuration = new Configuration();

        try (FileSystem fs = FileSystem.get(new URI(HDFS_URI), configuration, USER)) {
            // 删除
            boolean delete = fs.delete(new Path("/test/input2"), true);
            System.out.println("delete = " + delete);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
