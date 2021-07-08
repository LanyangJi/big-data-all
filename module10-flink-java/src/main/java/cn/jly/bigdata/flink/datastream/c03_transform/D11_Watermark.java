package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * 注意
 * 数据被flink线程处理，在多并行度的情况下，watermark对齐会取所有线程中最小的watermark来作为当前水印
 *
 * @author lanyangji
 * @date 2021/7/6 15:23
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D11_Watermark
 */
public class D11_Watermark {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
        int port = tool.getInt("port", 9999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 数据被flink线程处理，在多并行度的情况下，watermark对齐会取所有线程中最小的watermark来作为当前水印
        env.setParallelism(1);

        // 设置周期生成水印的时间间隔
        // env.getConfig().setAutoWatermarkInterval(5);

        env.socketTextStream(host, port)
                .map(new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String value) throws Exception {
                        return JSON.parseObject(value, SensorReading.class);
                    }
                })
                // 水印固定延迟2秒
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofMillis(2000))
                                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                                    @Override
                                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                        System.out.println(element);
                                        return element.getTimestamp();
                                    }
                                })
                )
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading value) throws Exception {
                        return value.getName();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
                .maxBy("temperature")
                .printToErr();

        env.execute("D11_Watermark");
    }
}
