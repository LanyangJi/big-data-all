package cn.jly.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;

/**
 * @author lanyangji
 * @date 2021/4/20 上午 11:25
 * @packageName cn.jly.hadoop.hdfs
 * @className Hdfs08IoUpload
 */
public class Hdfs08IoUpload extends BaseConfig {
    public static void main(String[] args) {
        init();

        Configuration configuration = new Configuration();
        try (
                FileSystem fs = FileSystem.get(new URI(HDFS_URI), configuration, USER);
                // 输入流
                FileInputStream fis = new FileInputStream(new File("e:/atguigu.log"));
                // hdfs 输出流
                FSDataOutputStream fos = fs.create(new Path("/atguigu.log"))
        ) {
            // 流对拷
            IOUtils.copyBytes(fis, fos, configuration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
