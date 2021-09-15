package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import cn.jly.bigdata.flink.datastream.c02_source.D06_CustomRandomSource;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

/**
 * @author lanyangji
 * @date 2021/7/6 13:34
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D08_SlidingWIndow
 */
public class D08_SlidingTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WindowedStream<Tuple2<String, Double>, String, TimeWindow> windowDS = env.addSource(new D06_CustomRandomSource.RandomSource(Arrays.asList("sensor-01",
                "sensor-02",
                "sensor-03",
                "sensor-04",
                "sensor-05",
                "sensor-06"
        )))
                .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading value) throws Exception {
                        return Tuple2.of(value.getName(), value.getTemperature());
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Double>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Double> value) throws Exception {
                        return value.f0;
                    }
                })
                // 每5秒触发统计过去15秒的数据
                .window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(5)));

        // 1. 增量聚合
        // 允许迟到5秒钟，窗口触发的时候先输出一个计算结果，但是不关闭窗口，等再过一分钟后，将迟到的数据参与聚合后输出计算结果
        // 来一条迟到数据，就在原来的窗口结果上聚合计算一次  -> 保证快速和计算结果的正确性
        // 这种场景一般都是基于event time才有意义
        OutputTag<Tuple2<String, Double>> lateOutputData = new OutputTag<Tuple2<String, Double>>("late_date"){};
        SingleOutputStreamOperator<Long> aggregateDS = windowDS.allowedLateness(Time.seconds(5))
                .sideOutputLateData(lateOutputData) // 将迟到超过5秒的数据写入侧输出流
                .aggregate(new CountAggregationFunction());

        // 正常输出
        aggregateDS.printToErr("count aggregate function");

        // 拿到侧输出流中的迟到数据
        DataStream<Tuple2<String, Double>> lateDs = aggregateDS.getSideOutput(lateOutputData);
        lateDs.print();

        // 2. 全窗口聚合
        windowDS.apply(new MyWindowFunction())
                .print("count window function");

        env.execute("D08_SlidingTimeWindow");
    }

    /**
     * 自定义增量聚合函数
     */
    public static class CountAggregationFunction implements AggregateFunction<Tuple2<String, Double>, Long, Long> {

        /**
         * 创建累加器
         *
         * @return
         */
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        /**
         * 累加
         *
         * @param value
         * @param accumulator
         * @return
         */
        @Override
        public Long add(Tuple2<String, Double> value, Long accumulator) {
            return accumulator + 1L;
        }

        /**
         * 获取累加器
         *
         * @param accumulator
         * @return
         */
        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        /**
         * 合并累加器
         *
         * @param a
         * @param b
         * @return
         */
        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * 全窗口处理函数
     * <p>
     * 收集完窗口的所有数据
     */
    public static class MyWindowFunction implements WindowFunction<Tuple2<String, Double>, Tuple2<String, Long>, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Double>> input, Collector<Tuple2<String, Long>> out) throws Exception {
            long count = 0;
            count += IteratorUtils.toList(input.iterator()).size();

            out.collect(Tuple2.of(key, count));
        }
    }
}