package cn.jly.bigdata.flink.datastream.c03_transform;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author jilanyang
 * @date 2021/7/1 16:00
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D01_Map
 */
public class D01_Map_FlatMap_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // word count案例，其实可以只用flatMap就能实现下面flatMap+map干的事情
        env.readTextFile("input/word.txt")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            if (StringUtils.isNotBlank(word)) {
                                out.collect(word);
                            }
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return Tuple2.of(value, 1L);
                    }
                })
                // DataStream -> KeyedStream ：逻辑地将一个流拆分成不相交的分区，每个分区包含具有相同 key 的元素，
                // 在内部以 hash 的形式实现的。
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .print();

        env.execute("D01_Map_FlatMap_KeyBy");
    }
}
