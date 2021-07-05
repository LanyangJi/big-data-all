package cn.jly.bigdata.flink.datastream.c03_transform;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Union和connect的区别
 * 1
 * Union 之前两个流的类型必须是一样， Connect 可以不一样，在之后的 coMap中再去调整成为一样的。
 * 2.
 * Connect只能操作两个流， Union可以操作多个
 *
 * @author jilanyang
 * @date 2021/7/1 16:38
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D04_Connect
 */
public class D04_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> firstDataStream = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> secondDataStream = env.fromElements("tom", "amy", "kobe");

        // connect
        ConnectedStreams<Integer, String> connectedStreams = firstDataStream.connect(secondDataStream);

        /*
        DataStream,DataStream -> ConnectedStreams ：连接两个保持他们类型的数
        据流，两个数据流被 Connect 之后，只是被放在了一个同一个流中，内部依然保持
        各自的数据和形式不发生任何变化，两个流相互独立。
         */
        connectedStreams.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value + ".suffix";
            }

            @Override
            public String map2(String value) throws Exception {
                return value + ".suffix";
            }
        })
                .print();

        env.execute("D04_Connect");
    }
}
