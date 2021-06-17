package cn.jly.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * 测试配置参数优先级
 *
 * @author lanyangji
 * @date 2021/4/20 上午 10:26
 * @packageName cn.jly.hadoop.hdfs
 * @className Hdfs02ParamsPriority
 */
public class Hdfs02ParamsPriorityAndUpload extends BaseConfig{
    public static void main(String[] args) {
        init();

        // 参数优先级排序：（1）客户端代码中设置的值 >（2）ClassPath下的用户自定义配置文件 >（3）然后是服务器的默认配置
        Configuration configuration = new Configuration();
        // 设置副本数
        configuration.set("dfs.replication", "2");

        try (
                FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:9000"), configuration, "lanyangji")
        ) {
            // 上传文件
            fileSystem.copyFromLocalFile(new Path("d:/SensorReading.txt"), new Path("/test/input/SensorReading.txt"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
