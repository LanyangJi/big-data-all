package cn.jly.hadoop.mapreduce;

import cn.jly.hadoop.hdfs.BaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 预合并
 *
 * @author lanyangji
 * @date 2021/4/22 下午 8:13
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr09CombinerDriver
 */
public class Mr09CombinerDriver extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);
        job.setJarByClass(Mr09CombinerDriver.class);
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\wordcount\\inputword"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\wordcount\\combinerOutput"));

        // 指定combiner类，combiner就是reducer的子类
        job.setCombinerClass(WcReducer.class);

        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    /**
     * mapper
     */
    public static class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text k = new Text();
        private final IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] words = value.toString().split(" ");
            for (String word : words) {
                k.set(word);
                context.write(k, v);
            }
        }
    }

    /**
     * reducer
     */
    public static class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable v = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            v.set(sum);
            context.write(key, v);
        }
    }
}
