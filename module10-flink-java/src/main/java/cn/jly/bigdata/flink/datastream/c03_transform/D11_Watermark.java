package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

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

        WindowedStream<SensorReading, String, TimeWindow> windowDS = env.socketTextStream(host, port)
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
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5000)));

        // 允许迟到数据，在watermark的基础上再允许数据最多迟到3秒
        // 允许迟到3秒钟，窗口触发的时候先输出一个计算结果，但是不关闭窗口，等再过一分钟后，将迟到的数据参与聚合后输出计算结果
        // 来一条迟到数据，就在原来的窗口结果上聚合计算一次  -> 保证快速和计算结果的正确性
        // 这种场景一般都是基于event time才有意义
        OutputTag<SensorReading> lateDataOutputTag = new OutputTag<>("late_data_output_tag");
        SingleOutputStreamOperator<SensorReading> maxDS = windowDS.allowedLateness(Time.seconds(3))
                .sideOutputLateData(lateDataOutputTag)
                .maxBy("temperature");

        // 正常输出
        maxDS.print("max");

        // 拿到迟到数据
        DataStream<SensorReading> lateDS = maxDS.getSideOutput(lateDataOutputTag);
        lateDS.printToErr("late");

        env.execute("D11_Watermark");
    }
}
