package cn.jly.bigdata.flink_advanced.datastream.c13_collector;

import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.lang.reflect.Parameter;
import java.util.Iterator;

/**
 * @author jilanyang
 * @date 2021/8/30 16:33
 */
public class D01_DataStreamCollector {
    @SneakyThrows
    public static void main(String[] args) {

        // 参数提取
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "localhost");
        int port = tool.getInt("port", 9999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1); // 便于调试观看

        // source
        SingleOutputStreamOperator<Tuple2<String, Long>> wcDS = env.socketTextStream(host, port)
                .flatMap(
                        new FlatMapFunction<String, Tuple2<String, Long>>() {
                            private final Tuple2<String, Long> t = new Tuple2<>();

                            @Override
                            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                                for (String word : value.split(" ")) {
                                    t.f0 = word;
                                    t.f1 = 1L;

                                    out.collect(t);
                                }
                            }
                        }
                )
                .keyBy(t -> t.f0)
                .sum(1);

        // 收集结果数据
        Iterator<Tuple2<String, Long>> iterator = wcDS.executeAndCollect();

        while (iterator.hasNext()) {
            Tuple2<String, Long> wc = iterator.next();
            System.out.println(wc);
        }

        env.execute("D01_DataStreamCollector");

    }
}
