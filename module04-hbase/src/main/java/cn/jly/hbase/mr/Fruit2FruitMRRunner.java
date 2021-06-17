package cn.jly.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 将 fruit 表中的一部分数据，通过 MR 迁入到 fruit_mr 表中
 *
 * @author lanyangji
 * @date 2021/5/20 下午 5:08
 * @packageName cn.jly.hbase.mr
 * @className Fruit2FruitMRRunner
 */
public class Fruit2FruitMRRunner extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        // 获取配置
        final Configuration conf = this.getConf();
        // 创建job任务
        final Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(Fruit2FruitMRRunner.class);

        // 配置job
        final Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

        // 设置 Mapper，注意导入的是 mapreduce 包下的，不是 mapred 包下的，后者是老版本
        TableMapReduceUtil.initTableMapperJob(
                "fruit", // 表名
                scan, // 初始操作
                ReadFruitMapper.class, // mapper类
                ImmutableBytesWritable.class, // map输出的key类型
                Put.class, // map输出的value类型
                job // 设置给哪个job
        );

        // 设置reducer
        TableMapReduceUtil.initTableReducerJob(
                "fruit_mr", // 目标表名
                WriteFruitReducer.class, // reducer类
                job // 设置给哪个job
        );

        // reducer的个数
        job.setNumReduceTasks(1);

        final boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess) {
            throw new IOException("job running with error...");
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        final Configuration configuration = HBaseConfiguration.create();
        final int status = ToolRunner.run(configuration, new Fruit2FruitMRRunner(), args);
        System.exit(status);
    }
}
