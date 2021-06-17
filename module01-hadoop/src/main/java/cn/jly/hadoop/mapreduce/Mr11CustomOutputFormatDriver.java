package cn.jly.hadoop.mapreduce;

import cn.jly.hadoop.hdfs.BaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 过滤输入的log日志，包含atguigu的网站输出到e:/atguigu.log，不包含atguigu的网站输出到e:/other.log
 *
 * @author lanyangji
 * @date 2021/4/22 下午 10:34
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr11CustomOutputFormatDriver
 */
public class Mr11CustomOutputFormatDriver extends BaseConfig {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        init();

        final Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Mr11CustomOutputFormatDriver.class);
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\outputformat\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\outputformat\\output"));

        // 修改默认的输出outputFormat
        job.setOutputFormatClass(FilterFileOutputFormat.class);

        job.waitForCompletion(true);
    }

    /**
     * 自定义fileOutputFormat
     */
    public static class FilterFileOutputFormat extends FileOutputFormat<Text, NullWritable> {

        @Override
        public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new FilterRecordWriter(taskAttemptContext);
        }
    }

    /**
     * 自定义recordWriter
     */
    public static class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
        private FSDataOutputStream googleOs = null;
        private FSDataOutputStream otherOs = null;

        public FilterRecordWriter(TaskAttemptContext context) {
            // 文件系统
            FileSystem fs = null;
            try {
                fs = FileSystem.get(context.getConfiguration());

                // 输出路径
                final Path googlePath = new Path("e:/google.txt");
                final Path otherPath = new Path("e:/other.txt");

                // 创建输出流
                googleOs = fs.create(googlePath);
                otherOs = fs.create(otherPath);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
            if (key.toString().contains("google")) {
                googleOs.write(key.toString().getBytes(StandardCharsets.UTF_8));
            } else {
                otherOs.write(key.toString().getBytes(StandardCharsets.UTF_8));
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            IOUtils.closeStream(googleOs);
            IOUtils.closeStream(otherOs);
        }
    }

    public static class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String line = key.toString();
            line += "\r\n";
            key.set(line);

            context.write(key, NullWritable.get());
        }
    }
}
