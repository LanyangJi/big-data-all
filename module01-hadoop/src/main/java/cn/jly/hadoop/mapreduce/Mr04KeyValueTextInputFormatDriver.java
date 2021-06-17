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
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计输入文件中每一行的第一个单词相同的行数
 * 输入
 * banzhang ni hao
 * xihuan hadoop banzhang
 * banzhang ni hao
 * xihuan hadoop banzhang
 *
 * @author lanyangji
 * @date 2021/4/21 上午 9:33
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr04KeyValueTextInputFormat
 */
public class Mr04KeyValueTextInputFormatDriver extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Configuration configuration = new Configuration();
        // 配置k v分隔符
        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        final Job job = Job.getInstance(configuration);
        job.setJarByClass(Mr04KeyValueTextInputFormatDriver.class);
        job.setMapperClass(KvTextMapper.class);
        job.setReducerClass(KvTextReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\kvtext\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\kvtext\\output"));
        // 设置inputFormat为KeyValueTextInputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        final boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }

    /**
     * mapper
     */
    public static class KvTextMapper extends Mapper<Text, Text, Text, IntWritable> {
        private final IntWritable v = new IntWritable(1);

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, v);
        }
    }

    /**
     * reducer
     */
    public static class KvTextReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
