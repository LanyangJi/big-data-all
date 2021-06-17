package cn.jly.hadoop.hdfs;

/**
 * @author lanyangji
 * @date 2021/4/20 上午 10:35
 * @packageName cn.jly.hadoop.hdfs
 * @className BaseConfig
 */
public class BaseConfig {
    public static final String HDFS_URI = "hdfs://node01:9000";
    public static final String USER = "lanyangji";

    public static void init() {
        System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop-3.1.0");
    }
}
