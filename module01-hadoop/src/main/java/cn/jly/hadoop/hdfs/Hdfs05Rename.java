package cn.jly.hadoop.hdfs;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author lanyangji
 * @date 2021/4/20 上午 10:52
 * @packageName cn.jly.hadoop.hdfs
 * @className Hdfs05Rename
 */
public class Hdfs05Rename extends BaseConfig{
    public static void main(String[] args) {
        init();

        Configuration configuration = new Configuration();
        try (FileSystem fs = FileSystem.get(new URI(HDFS_URI), configuration, USER)) {
            // 重命名
            boolean rename = fs.rename(new Path("/test/input/SensorReading.txt"), new Path("/test/input/_delete.txt"));
            System.out.println("rename = " + rename);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
