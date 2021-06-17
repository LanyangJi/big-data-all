package cn.jly.hadoop.mapreduce;

import cn.jly.hadoop.hdfs.BaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author lanyangji
 * @date 2021/4/20 下午 10:26
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr03CombineInputFormat
 */
public class Mr03CombineTextInputFormatDriver extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);
        job.setJarByClass(Mr03CombineTextInputFormatDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\wordcount\\inputword"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\wordcount\\outputword2"));

        // 设置InputFormat，默认是TextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 设置虚拟存储最大值 4M
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        final boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
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
