package cn.jly.hadoop.mapreduce;

import cn.jly.hadoop.hdfs.BaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author lanyangji
 * @date 2021/4/20 下午 3:53
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr01WordCount
 */
public class Mr01WordCountDriver extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        // 获取配置信息以及封装任务
        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);

        // 设置jar的加载路径
        job.setJarByClass(Mr01WordCountDriver.class);

        // 关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置mapper输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置最终输出
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\wordcount\\inputword"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\wordcount\\output"));

        // 提交任务
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    /**
     * mapper
     */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text k = new Text();
        private final LongWritable v = new LongWritable(1);

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
    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable v = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }

            v.set(sum);
            context.write(key, v);
        }
    }
}
