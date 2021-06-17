package cn.jly.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * 文件下载
 *
 * @author lanyangji
 * @date 2021/4/20 上午 10:41
 * @packageName cn.jly.hadoop.hdfs
 * @className Hdfs03FileDownload
 */
public class Hdfs03FileDownload extends BaseConfig {
    public static void main(String[] args) {
        init();

        Configuration configuration = new Configuration();
        try (
                FileSystem fs = FileSystem.get(new URI("hdfs://node01:9000"), configuration, "lanyangji")
        ) {
            /*
                下载文件
                boolean delSrc 指是否将原文件删除
                Path src 指要下载的文件路径
                Path dst 指将文件下载到的路径
                boolean useRawLocalFileSystem 是否开启文件校验
             */
            fs.copyToLocalFile(false,
                    new Path("/test/input/SensorReading.txt"),
                    new Path("e:/test1.txt"),
                    true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
