package cn.jly.bigdata.flink.datastream.c03_transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author jilanyang
 * @date 2021/7/1 16:06
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D02_Filter
 */
public class D02_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements("words tom lily kobe")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(word);
                        }
                    }
                })
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.contains("o");
                    }
                })
                .print();

        env.execute("D02_Filter");
    }
}
