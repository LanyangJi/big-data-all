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
import java.security.PrivateKey;
import java.util.StringJoiner;

/**
 * 主人：好友们
 * A:B,C,D,F,E,O
 * B:A,C,E,K
 * C:F,A,D,I
 * D:A,E,F,L
 * E:B,C,D,M,L
 * F:A,B,C,D,E,O,M
 * G:A,C,D,E,F
 * H:A,C,D,E,O
 * I:A,O
 * J:B,O
 * K:A,C,D
 * L:D,E,F
 * M:E,F,G
 * O:A,H,I,J
 * <p>
 * 找共同朋友思路
 * 1. 转换成谁谁谁是哪些人的朋友
 * mapper
 * 主人	好友
 * A	B
 * A    C
 * ……
 * B	A
 * <p>
 * reducer
 * 好友	主人们
 * A	B,C,D,F,G,H,I,K,O	即A是右面这些人的朋友
 * <p>
 * 2.
 * mapper
 * 主人-主人	共同好友
 * B-C		A
 * B-D		A
 * B-F		A
 * ……
 * <p>
 * reducer
 * B-C		A,……
 *
 * @author lanyangji
 * @date 2021/4/28 上午 10:15
 * @packageName cn.jly.hadoop.mapreduce
 * @className Mr18BothFriendsDriver1
 */
public class Mr18BothFriendsDriver1 extends BaseConfig {
    public static void main(String[] args) throws Exception {
        init();

        final Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Mr18BothFriendsDriver1.class);
        job.setMapperClass(BothFriendsFirstJobMapper.class);
        job.setReducerClass(BothFriendsFirstJobReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\bothfriends\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\bothfriends\\output"));
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    public static class BothFriendsFirstJobMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] fields = value.toString().split(":");
            // 主人
            String owner = fields[0];
            // 朋友们
            final String[] friends = fields[1].split(",");
            for (String friend : friends) {
                // 朋友   主人
                context.write(new Text(friend), new Text(owner));
            }
        }
    }

    public static class BothFriendsFirstJobReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            final StringJoiner stringJoiner = new StringJoiner(",");
            for (Text value : values) {
                stringJoiner.add(value.toString());
            }

            // 朋友   主人们
            context.write(key, new Text(stringJoiner.toString()));
        }
    }
}
