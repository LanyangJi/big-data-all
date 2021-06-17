package cn.jly.hbase.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 实现将 HDFS 中的数据写入到 Hbase 表中。
 *
 * @author lanyangji
 * @date 2021/5/20 下午 5:23
 * @packageName cn.jly.hbase.Mr2
 * @className ReadFruitFromHDFSMapper
 */
public class ReadFruitFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        // 取值
        String rowKey = fields[0];
        String name = fields[1];
        String color = fields[2];

        // key
        ImmutableBytesWritable k = new ImmutableBytesWritable(Bytes.toBytes(rowKey));
        // value
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(color));

        context.write(k, put);
    }
}
