package cn.jly.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author lanyangji
 * @date 2021/4/20 上午 11:15
 * @packageName cn.jly.hadoop.hdfs
 * @className Hdfs07FileStatus
 */
public class Hdfs07FileStatus extends BaseConfig{
    public static void main(String[] args) {
        init();

        Configuration configuration = new Configuration();
        try (FileSystem fs = FileSystem.get(new URI(HDFS_URI), configuration, USER)) {
            // 判断文件还是文件夹
            FileStatus[] fileStatuses = fs.listStatus(new Path("/test/input/"));
            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isFile()) {
                    System.out.println("f: " + fileStatus.getPath().getName());
                } else {
                    System.out.println("d: " + fileStatus.getPath().getName());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
