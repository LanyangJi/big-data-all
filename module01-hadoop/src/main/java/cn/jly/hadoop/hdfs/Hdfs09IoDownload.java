package cn.jly.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.jboss.netty.channel.ExceptionEvent;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * @author lanyangji
 * @date 2021/4/20 下午 1:37
 * @packageName cn.jly.hadoop.hdfs
 * @className Hdfs09IoDownload
 */
public class Hdfs09IoDownload extends BaseConfig {
    public static void main(String[] args) {
        init();

        Configuration configuration = new Configuration();
        try (
                FileSystem fs = FileSystem.get(new URI(HDFS_URI), configuration, USER);
                // hdfs 输入流
                FSDataInputStream fis = fs.open(new Path("/atguigu.log"));
                // 输出流
                final FileOutputStream fos = new FileOutputStream(new File("e:/atguigu2.log"))
        ) {
            // 流对拷
            IOUtils.copyBytes(fis, fos, configuration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
