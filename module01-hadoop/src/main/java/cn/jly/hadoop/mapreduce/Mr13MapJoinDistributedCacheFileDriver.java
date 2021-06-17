package cn.jly.hadoop.mapreduce;

import cn.jly.hadoop.hdfs.BaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * reduce join优化：
 * 分布式缓存小文件
 * 在map端进行join
 *
 * @author lanyangji
 * @date 2021/4/24 下午 3:34
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr13MapJoinDriver
 */
public class Mr13MapJoinDistributedCacheFileDriver extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Mr13MapJoinDistributedCacheFileDriver.class);
        job.setMapperClass(DistributedCacheFileMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\mapjoin\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\mapjoin\\output"));

        job.addCacheFile(new URI("file:///E:/mapjoin/inputcache/pd.txt"));
        job.setNumReduceTasks(0);

        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    public static class DistributedCacheFileMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        /**
         * 存储产品id和产品名称
         */
        private final HashMap<String, String> productMap = new HashMap<>();
        private final Text k = new Text();

        /**
         * 读取分布式缓存文件
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            final URI[] cacheFiles = context.getCacheFiles();
            final String productFilePath = cacheFiles[0].getPath();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(productFilePath)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    final String[] fields = line.split("\t");
                    productMap.put(fields[0].trim(), fields[1].trim());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] fields = value.toString().split("\t");
            String orderId = fields[0].trim();
            String productId = fields[1].trim();
            String amount = fields[2].trim();

            final String productName = productMap.get(productId);
            k.set(String.join("\t", orderId, productName, amount));

            context.write(k, NullWritable.get());
        }
    }
}
