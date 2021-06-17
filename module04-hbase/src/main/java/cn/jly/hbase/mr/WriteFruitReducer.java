package cn.jly.hbase.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * 将读取到的 fruit 表中的数据写入到 fruit_mr 表 中
 *
 * @author lanyangji
 * @date 2021/5/20 下午 5:04
 * @packageName cn.jly.hbase.mr
 * @className WriteFruitReducer
 */
public class WriteFruitReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put value : values) {
            // 将每一行的数据写到目标表中
            context.write(NullWritable.get(), value);
        }
    }
}
