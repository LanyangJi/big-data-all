package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 事件时间 + 水印 + 迟到数据处理
 *
 * @author lanyangji
 * @date 2021/7/14 10:32
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D12_Exer_EventTimeWindow_Watermark_Lateness
 */
public class D12_Exer_EventTimeWindow_Watermark_Lateness {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
        int port = tool.getInt("port", 9999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 迟到数据侧输出流outputTag
        OutputTag<SensorReading> lateDataOutputTag = new OutputTag<SensorReading>("late_data_output_tag"){};

        SingleOutputStreamOperator<SensorReading> aggregateDS =
                env.socketTextStream(host, port)
                        .map(new Json2ObjectMapFunction())
                        .assignTimestampsAndWatermarks(
                                // 允许两秒延迟
                                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                        .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                                            @Override
                                            public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                                return element.getTimestamp();
                                            }
                                        })
                        )
                        // 按照name分组
                        .keyBy(new KeySelector<SensorReading, String>() {
                            @Override
                            public String getKey(SensorReading value) throws Exception {
                                return value.getName();
                            }
                        })
                        // 5秒的滚动窗口
                        .window(
                                TumblingEventTimeWindows.of(Time.seconds(5))
                        )
                        // 窗口之外允许迟到10秒数据
                        .allowedLateness(Time.seconds(10))
                        .sideOutputLateData(lateDataOutputTag)
                        .minBy("temperature");

        /*
            开窗计算公式 - TimeWindow类中，
            public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
                return timestamp - (timestamp - offset + windowSize) % windowSize;
            }

            offset默认为0（也可以自己传参数指定），默认算的的结果是：
                    最靠近当前timestamp的（小于等于）的windowSize的整数倍时间戳
                    且窗口边界是左闭右开
         */
        // 正常输出聚合结果
        aggregateDS.print("minTemp");

        // 拿去侧输出流中最终的迟到数据 -> 一般来说，常规结果数据会被写到外部系统，比如mysql或者redis，这里可以连接外部系统
        // 读取到先前的聚合结果，然后将迟到数据与先前的结果聚合，得到最终的正确结果
        // !!! 三重保障：1. watermark; 2. allowedLateness
        DataStream<SensorReading> lateDS = aggregateDS.getSideOutput(lateDataOutputTag);
        lateDS.printToErr("late data");

        env.execute("D12_Exer_EventTimeWindow_Watermark_Lateness");

    }

    /**
     * mapFunction：将读取到的json数据转换为对应的实体类型
     */
    public static final class Json2ObjectMapFunction implements MapFunction<String, SensorReading> {

        @Override
        public SensorReading map(String value) throws Exception {
            return JSON.parseObject(value, SensorReading.class);
        }
    }
}
