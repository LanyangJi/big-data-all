package cn.jly.bigdata.flink_advanced.datastream.c03_transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * connect和union的异同点
 * * 相同点：用来合并流
 * * 不同点：
 * * 1. union通常叫合并，connect通常叫连接
 * * 2. union只能合并相同数据类型的流；connect可以连接不同数据类型的流
 * * 3. union可以合并多个数据流；connect只能是一个流连接另一个流
 * * 4. connect连接之后是ConnectedStreams，ConnectedStreams会对两个流中的数据应用不同的处理方法，且双流之间可以共享状态
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c03_transformation
 * @class D03_Union
 * @date 2021/7/25 21:33
 */
public class D02_Union_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> ds1 = env.fromElements("hadoop", "spark", "scala");
        DataStreamSource<String> ds2 = env.fromElements("flume", "flink", "kafka");
        DataStreamSource<Integer> ds3 = env.fromElements(1, 2, 3);

        ds1.union(ds2).map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).print();

        // 两个方法分开处理
        ds1.connect(ds3).map(new CoMapFunction<String, Integer, String>() {
                    @Override
                    public String map1(String s) throws Exception {
                        return s;
                    }

                    @Override
                    public String map2(Integer i) throws Exception {
                        return i.toString();
                    }
                })
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        return s;
                    }
                })
                .printToErr();


        env.execute("D03_Union");
    }
}
