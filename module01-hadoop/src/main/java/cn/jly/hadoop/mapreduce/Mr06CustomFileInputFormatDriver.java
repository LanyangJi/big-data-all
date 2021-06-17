package cn.jly.hadoop.mapreduce;

import cn.jly.hadoop.hdfs.BaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * 将多个小文件合并成一个SequenceFile文件（SequenceFile文件是Hadoop用来存储二进制形式的key-value对的文件格式），
 * SequenceFile里面存储着多个文件，存储的形式为文件路径+名称为key，文件内容为value。
 *
 * @author lanyangji
 * @date 2021/4/21 下午 4:47
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr06CustomFileInputFormat
 */
public class Mr06CustomFileInputFormatDriver extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);
        job.setJarByClass(Mr06CustomFileInputFormatDriver.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\customfileinput\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\customfileinput\\output"));

        // 修改默认的InputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);
        // 修改输入的文件格式outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        final boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }

    /**
     * 自定义FileInputFormat
     */
    public static class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {

        @Override
        public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            final WholeRecordReader recordReader = new WholeRecordReader();
            recordReader.initialize(inputSplit, taskAttemptContext);
            return recordReader;
        }
    }

    /**
     * 自定义RecordReader
     */
    public static class WholeRecordReader extends RecordReader<Text, BytesWritable> {
        // 配置
        private Configuration configuration;
        // 切片
        private FileSplit fileSplit;
        // 正在处理标志位
        private boolean isProgress = true;
        // value
        private final BytesWritable value = new BytesWritable();
        // key
        private final Text key = new Text();

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            this.configuration = context.getConfiguration();
            this.fileSplit = (FileSplit) inputSplit;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (isProgress) {
                // 定义缓冲区
                final byte[] buffer = new byte[(int) this.fileSplit.getLength()];
                final Path filePath = this.fileSplit.getPath();
                try (
                        // 获取文件系统
                        FileSystem fileSystem = filePath.getFileSystem(configuration);
                        // 创建输入流
                        final FSDataInputStream fis = fileSystem.open(filePath)
                ) {
                    // 读取文件内容
                    IOUtils.readFully(fis, buffer, 0, buffer.length);
                    // value赋值
                    value.set(buffer, 0, buffer.length);
                    // key赋值
                    key.set(filePath.toString());
                }
                isProgress = false;
                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public BytesWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {

        }
    }

    /**
     * mapper
     */
    public static class SequenceFileMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    /**
     * reducer
     */
    public static class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            // value只有一个
            context.write(key, values.iterator().next());
        }
    }
}
