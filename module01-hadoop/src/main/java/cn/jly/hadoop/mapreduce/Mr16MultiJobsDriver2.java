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
 * @date 2021/4/27 下午 5:10
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr16MultiJobsDriver2
 */
public class Mr16MultiJobsDriver2 extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Mr16MultiJobsDriver2.class);
        job.setMapperClass(SecondJobMapper.class);
        job.setReducerClass(SecondJobReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\jobs\\output"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\jobs\\output2"));
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    public static class SecondJobMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] fields = value.toString().split("\\$");
            final String word = fields[0];
            final String fileName = fields[1].split("\t")[0];
            final String count = fields[1].split("\t")[1];

            k.set(word);
            v.set(String.join("->", fileName, count));
            context.write(k, v);
        }
    }

    public static class SecondJobReducer extends Reducer<Text, Text, Text, Text> {
        private Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            final StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value).append("\t");
            }

            v.set(sb.toString());
            context.write(key, v);
        }
    }
}
