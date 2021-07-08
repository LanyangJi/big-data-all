package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author lanyangji
 * @date 2021/7/6 17:59
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D04_SessionWindow
 */
public class D04_SessionWindow {
    public static void main(String[] args) throws Exception {

        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
        int port = tool.getInt("port", 9999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream(host, port)
                .map(new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String s) throws Exception {
                        return JSON.parseObject(s, SensorReading.class);
                    }
                })
                // 允许迟到2秒
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofMillis(2000))
                                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                                    @Override
                                    public long extractTimestamp(SensorReading sensorReading, long l) {
                                        return sensorReading.getTimestamp();
                                    }
                                })
                )
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading sensorReading) throws Exception {
                        return sensorReading.getName();
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(3000)))
                .maxBy("temperature")
                .printToErr();

        env.execute("D04_SessionWindow");
    }
}
