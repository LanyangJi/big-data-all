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
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 对每个单词进行个数统计，要求根据每个输入文件的行数来规定输出多少个切片。此案例要求每三行放入一个切片中
 *
 * @author lanyangji
 * @date 2021/4/21 下午 4:00
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr05NLineInputFormatDriver
 */
public class Mr05NLineInputFormatDriver extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);
        job.setJarByClass(Mr05NLineInputFormatDriver.class);
        job.setMapperClass(NLineInputFormatMapper.class);
        job.setReducerClass(NLineInputFormatReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\nline\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\nline\\output"));

        // 设置NLineInputFormat替换默认的TextInputFormat
        job.setInputFormatClass(NLineInputFormat.class);
        // 设置3行为一个切片
        NLineInputFormat.setNumLinesPerSplit(job, 3);

        final boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }

    /**
     * mapper
     */
    public static class NLineInputFormatMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
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

    public static final class NLineInputFormatReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable v = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable value : values) {
                sum += value.get();
            }
            v.set(sum);

            context.write(key, v);
        }
    }
}
