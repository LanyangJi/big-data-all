package cn.jly.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * 客户端连接
 *
 * @author lanyangji
 * @date 2021/4/20 上午 10:03
 * @packageName cn.jly.hadoop.hdfs
 * @className Hdfs01Client
 */
public class Hdfs01Client extends BaseConfig {
    public static void main(String[] args) {
        init();

        Configuration configuration = new Configuration();
        try (
                FileSystem fs = FileSystem.get(
                        new URI("hdfs://node01:9000"),
                        configuration,
                        "lanyangji"
                )
        ) {
            // 创建目录
            boolean mkdirs = fs.mkdirs(new Path("/test/input2"));
            System.out.println("mkdirs = " + mkdirs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
