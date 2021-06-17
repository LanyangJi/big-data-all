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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 多job串联
 * <p>
 * 输入words
 * 输出：
 * word1   a.txt->3    b.txt->4    c.txt->5
 * word2 ...
 *
 * @author lanyangji
 * @date 2021/4/27 下午 3:31
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr16MultiJobsDriver01
 */
public class Mr16MultiJobsDriver01 extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Mr16MultiJobsDriver01.class);
        job.setMapperClass(FirstJobMapper.class);
        job.setReducerClass(FirstJobReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\jobs\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\jobs\\output"));
        job.waitForCompletion(true);
    }

    /**
     * mapper
     */
    public static class FirstJobMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private String fileName;
        private Text k = new Text();
        private IntWritable v = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            final FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] words = value.toString().split(" ");
            for (String word : words) {
                k.set(String.join("$", word, fileName));
                context.write(k, v);
            }
        }
    }

    public static class FirstJobReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable v = new IntWritable();

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
