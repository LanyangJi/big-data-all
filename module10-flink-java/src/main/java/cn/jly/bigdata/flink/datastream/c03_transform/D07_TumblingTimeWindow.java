package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import cn.jly.bigdata.flink.datastream.c02_source.D06_CustomRandomSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * 滚动窗口
 *
 * @author lanyangji
 * @date 2021/7/6 11:03
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D07_TumblingWindow
 */
public class D07_TumblingTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new D06_CustomRandomSource.RandomSource(Arrays.asList(
                "sensor-01",
                "sensor-02",
                "sensor-03",
                "sensor-04",
                "sensor-05",
                "sensor-06"
        )))
                .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading value) throws Exception {
                        Tuple2<String, Double> tuple2 = Tuple2.of(value.getName(), value.getTemperature());
                        System.out.println(tuple2);
                        return tuple2;
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Double>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Double> value) throws Exception {
                        return value.f0;
                    }
                })
                // 5秒钟基于处理时间的滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 拿到5秒之内的最高温度
                .maxBy(1)
                .printToErr();

        env.execute("D07_TumblingTimeWindow");
    }
}
