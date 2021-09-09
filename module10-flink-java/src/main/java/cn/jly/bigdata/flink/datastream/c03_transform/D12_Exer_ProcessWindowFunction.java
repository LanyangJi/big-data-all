package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.hutool.core.collection.CollUtil;
import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * 需求：监控温度传感器的温度值，如果温度值在10 秒钟之内 (event time)连续上升， 则报警。
 * 考虑乱序数据
 * <p>
 * 注意：ReduceFunction和AggregateFunction存空间损耗来讲，是要优于ProcessWindowFunction
 * ReduceFunction 和 AggregateFunction 可以显着降低存储需求，因为它们急切地聚合元素并且每个窗口只存储一个值。
 * 相比之下，仅使用 ProcessWindowFunction 需要累积所有元素。
 *
 * @author lanyangji
 * @date 2021/7/6 19:31
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D12_TimeService
 */
public class D12_Exer_ProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
        int port = tool.getInt("port", 9999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream(host, port)
                // json转对象
                .map(new Json2ObjectMapFunction())
                // 水印，乱序数据允许迟到2秒
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofMillis(2000))
                                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                                    @Override
                                    public long extractTimestamp(SensorReading sensorReading, long l) {
                                        return sensorReading.getTimestamp();
                                    }
                                })
                )
                // 以name分组
                .keyBy(new NameKeySelector())
                // 滑动窗口5s
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
                // ProcessWindowFunction
                .process(new TemperatureWindowFunction())
                .printToErr();

        env.execute("D12_Exer_ProcessWindowFunction");
    }

    // ProcessWindowFunction
    public static final class TemperatureWindowFunction extends ProcessWindowFunction<SensorReading, String, String, TimeWindow> {
        /**
         * @param key
         * @param context
         * @param elements
         * @param out
         * @throws Exception
         */
        @Override
        public void process(String key, Context context, Iterable<SensorReading> elements, Collector<String> out) throws Exception {
            Collection<SensorReading> sensorReadings = CollUtil.addAll(new ArrayList<SensorReading>(), elements.iterator());
            // 按照时间戳排序
            List<SensorReading> sortList = CollUtil.sort(sensorReadings, Comparator.comparing(SensorReading::getTimestamp));
            boolean ascending = true;
            for (int i = 0; i < sortList.size() - 1; i++) {
                for (int j = i + 1; j < sortList.size(); j++) {
                    if (sortList.get(j).getTemperature() < sortList.get(i).getTemperature()) {
                        ascending = false;
                        break;
                    }
                }
            }

            // 温度持续升高
            if (ascending) {
                out.collect(key);
            }
        }
    }

    // key selector
    public static final class NameKeySelector implements KeySelector<SensorReading, String> {
        @Override
        public String getKey(SensorReading sensorReading) throws Exception {
            return sensorReading.getName();
        }
    }

    // json -> obj
    public static final class Json2ObjectMapFunction implements MapFunction<String, SensorReading> {
        @Override
        public SensorReading map(String s) throws Exception {
            return JSON.parseObject(s, SensorReading.class);
        }
    }
}
