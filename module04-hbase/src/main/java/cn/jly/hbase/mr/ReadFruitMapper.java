package cn.jly.hbase.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 将 fruit 表中的一部分数据，通过 MR 迁入到 fruit_mr 表中
 * ImmutableBytesWritable 输出的key的类型
 * Put 输出的value类型
 *
 * @author lanyangji
 * @date 2021/5/20 下午 4:30
 * @packageName cn.jly.hbase.mr
 * @className ReadFruitMapper
 */
public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put> {
    /**
     * @param key     输入的key，这里就是rowKey
     * @param value   输入的value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 将读入的表的记录的每一行封装到put中，这里只提取name和color字段
        final Put put = new Put(key.get());
        for (Cell cell : value.rawCells()) {
            if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                if ("name".equals(column) || "color".equals(column)) {
                    put.add(cell);
                }
            }
        }

        context.write(key, put);
    }
}
