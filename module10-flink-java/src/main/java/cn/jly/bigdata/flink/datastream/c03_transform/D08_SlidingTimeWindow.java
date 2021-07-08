package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import cn.jly.bigdata.flink.datastream.c02_source.D06_CustomRandomSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

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

        env.addSource(new D06_CustomRandomSource.RandomSource(Arrays.asList("sensor-01",
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
                .window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(5)))
                .maxBy(1)
                .printToErr();

        env.execute("D08_SlidingTimeWindow");
    }
}
