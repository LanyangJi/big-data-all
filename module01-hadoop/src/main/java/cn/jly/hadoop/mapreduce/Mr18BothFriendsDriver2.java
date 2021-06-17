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
import java.util.StringJoiner;

/**
 * @author lanyangji
 * @date 2021/4/28 下午 3:23
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr18BothFriendsDriver2
 */
public class Mr18BothFriendsDriver2 extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Mr18BothFriendsDriver1.class);
        job.setMapperClass(BothFriendsSecondJobMapper.class);
        job.setReducerClass(BothFriendsSecondJobReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\bothfriends\\output"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\bothfriends\\output2"));
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    public static class BothFriendsSecondJobMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] fields = value.toString().split("\t");
            String friend = fields[0];
            final String[] owners = fields[1].split(",");
            for (int i = 0; i < owners.length - 1; i++) {
                for (int j = i + 1; j < owners.length; j++) {
                    // 输出：主人1-主人2   好友
                    context.write(new Text(getOrderedKey(owners[i], owners[j])), new Text(friend));
                }
            }
        }

        private String getOrderedKey(String str1, String str2) {
            if (str1.compareTo(str2) < 0) {
                return String.join("-", str1, str2);
            }

            return String.join("-", str2, str1);
        }
    }

    public static class BothFriendsSecondJobReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringJoiner stringJoiner = new StringJoiner(",");
            for (Text value : values) {
                stringJoiner.add(value.toString());
            }

            context.write(key, new Text(stringJoiner.toString()));
        }
    }
}
