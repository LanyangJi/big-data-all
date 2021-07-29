package cn.jly.bigdata.flink_advanced.datastream.c06_4cores_window;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * watermark
 * 就是给数据再额外地加一个时间列，即watermark本质上就是一个时间戳
 * 目的：通过改变窗口触发计算时机来解决基于事件时间的数据的延迟或者乱序问题
 * <p>
 * 计算公式：
 * watermark = 当前窗口到达的（一批）数据的最大事件时间 - 最大允许的延迟（乱序）时间
 * 这样可以保证watermark水位线是保持一直上升（变大），不会下降（小于上次watermark的则不会生成）
 * !!! watermark是用来触发窗口计算的！！！！！！！！！
 * <p>
 * watermark如何触发窗口计算？
 * 窗口计算的触发条件：
 * 1. 窗口有数据；2. watermark >= 窗口的结束时间
 * watermark = 当前窗口到达的（一批）数据的最大事件时间 - 最大允许的延迟（乱序）时间
 * <p>
 * 注意:
 * 上面的触发公式进行如下变形:
 * watermark >= 窗口的结束时间
 * watermark = 当前窗口的最大的事件时间  -  最大允许的延迟时间或乱序时间
 * 当前窗口的最大的事件时间  -  最大允许的延迟时间或乱序时间  >= 窗口的结束时间
 * 当前窗口的最大的事件时间  >= 窗口的结束时间 +  最大允许的延迟时间或乱序时间
 * <p>
 * !!!!! 注意
 * 数据被flink线程处理，在多并行度的情况下，watermark对其会取所有线程中最小的watermark来作为当前水印
 *
 * <p>
 * <p>
 * 需求：（基于事件时间的滚动窗口）
 * 每个5秒，计算5秒内，每个用户的订单总额，并借助watermark解决一定程度上的数据延迟和乱序
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c06_4cores_window
 * @class D06_EventTime_Watermark
 * @date 2021/7/29 21:30
 */
public class D06_EventTime_Watermark {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
        int port = tool.getInt("port", 9999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 为了测试，设置并行度为1，
        // 数据被flink线程处理，在多并行度的情况下，watermark对其会取所有线程中最小的watermark来作为当前水印
        env.setParallelism(1);

        // order source
        SingleOutputStreamOperator<Order> orderDs = env.socketTextStream(host, port, ",")
                .map(new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Order(
                                fields[0],
                                fields[1],
                                Long.parseLong(fields[2].trim()),
                                Double.parseDouble(fields[3].trim())
                        );
                    }
                });

        // 开窗
        SingleOutputStreamOperator<Tuple2<String, Double>> windowDs =
                orderDs.assignTimestampsAndWatermarks(
                                // 允许3秒的数据延迟和乱序
                                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.of(3, ChronoUnit.SECONDS))
                                        .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                                            @Override
                                            public long extractTimestamp(Order order, long recordTimestamp) {
                                                return order.getTimestamp();
                                            }
                                        })

                        )
                        .keyBy(Order::getUserId)
                        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                        .apply(new WindowFunction<Order, Tuple2<String, Double>, String, TimeWindow>() {
                            @Override
                            public void apply(String key, TimeWindow timeWindow, Iterable<Order> iterable, Collector<Tuple2<String, Double>> collector) throws Exception {
                                double sum = 0;
                                for (Order order : iterable) {
                                    sum += order.getMoney();
                                }
                                collector.collect(Tuple2.of(key, sum));
                            }
                        });

        // 输出统计结果
        windowDs.printToErr();

        env.execute("D06_EventTime_Watermark");
    }
}
