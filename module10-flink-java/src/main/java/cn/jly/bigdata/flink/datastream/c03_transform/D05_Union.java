package cn.jly.bigdata.flink.datastream.c03_transform;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Union和connect的区别
 * 1
 * Union 之前两个流的类型必须是一样， Connect 可以不一样，在之后的 coMap中再去调整成为一样的。
 * 2.
 * Connect只能操作两个流， Union可以操作多个
 *
 * @author jilanyang
 * @date 2021/7/1 16:53
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D05_Unin
 */
public class D05_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source1 = env.fromElements("tom", "amy");
        DataStreamSource<String> source2 = env.fromElements("kobe", "amy");
        DataStreamSource<String> source3 = env.fromElements("tom", "james");

        // union -> 将数据类型相同的多个流合并成一个流
        DataStream<String> dataStream = source1.union(source2, source3);
        dataStream.print();

        env.execute("D05_Union");
    }
}
