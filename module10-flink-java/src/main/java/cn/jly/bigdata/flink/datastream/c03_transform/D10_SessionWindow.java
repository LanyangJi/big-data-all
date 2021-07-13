package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author lanyangji
 * @date 2021/7/6 17:59
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D04_SessionWindow
 */
public class D10_SessionWindow {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
        int port = tool.getInt("port", 9999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WindowedStream<SensorReading, String, TimeWindow> windowDS = env.socketTextStream(host, port)
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
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(3000)));

        // 允许迟到5秒钟，窗口触发的时候先输出一个计算结果，但是不关闭窗口，等再过一分钟后，将迟到的数据参与聚合后输出计算结果
        // 来一条迟到数据，就在原来的窗口结果上聚合计算一次  -> 保证快速和计算结果的正确性
        // 这种场景一般都是基于event time才有意义
        OutputTag<SensorReading> lateOutputData = new OutputTag<>("late_date");
        SingleOutputStreamOperator<SensorReading> maxDS = windowDS.allowedLateness(Time.seconds(5))
                .sideOutputLateData(lateOutputData) // 将迟到超过5秒的数据写入侧输出流
                .maxBy(1);

        // 正常输出
        maxDS.printToErr("count aggregate function");

        // 拿到侧输出流中的迟到数据
        DataStream<SensorReading> lateDs = maxDS.getSideOutput(lateOutputData);
        lateDs.print();

        env.execute("D10_SessionWindow");
    }
}
