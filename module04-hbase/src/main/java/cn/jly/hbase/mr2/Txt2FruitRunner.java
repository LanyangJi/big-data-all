package cn.jly.hbase.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 实现将 HDFS 中的数据写入到 Hbase 表中。
 *
 * @author lanyangji
 * @date 2021/5/20 下午 5:24
 * @packageName cn.jly.hbase.Mr2
 * @className Txt2FruitRunner
 */
public class Txt2FruitRunner extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(Txt2FruitRunner.class);

        // 输入路径
        Path inPath = new Path("hdfs://linux01:8020/input_fruit/fruit.tsv");
        FileInputFormat.addInputPath(job, inPath);

        // mapper
        job.setMapperClass(ReadFruitFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        // reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr2", WriteFruitMRFromTxtReducer.class, job);
        job.setNumReduceTasks(1);

        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess) {
            throw new RuntimeException("job running with error...");
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int run = ToolRunner.run(configuration, new Txt2FruitRunner(), args);
        System.exit(run);
    }
}
