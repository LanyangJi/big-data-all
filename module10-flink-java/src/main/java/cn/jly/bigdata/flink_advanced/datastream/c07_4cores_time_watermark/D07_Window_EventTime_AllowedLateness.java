package cn.jly.bigdata.flink_advanced.datastream.c07_4cores_time_watermark;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * allowedLateness搭配侧输出流处理迟到数据
 * 这里的迟到数据是指超过watermark机制允许的延迟时间而到达的数据
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c07_4cores_time_watermark
 * @class D06_Window_EventTime_AllowedLateness
 * @date 2021/7/31 9:33
 */
public class D07_Window_EventTime_AllowedLateness {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
        int port = tool.getInt("port", 9999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        SingleOutputStreamOperator<Order> orderDs = env.socketTextStream(host, port)
                // 数据类型转换
                .map(new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Order(fields[0], fields[1], Long.parseLong(fields[2]), Double.parseDouble(fields[3]));
                    }
                })
                // 指定时间戳和水印
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.of(3, ChronoUnit.SECONDS))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                                    @Override
                                    public long extractTimestamp(Order order, long recordTimestamp) {
                                        return order.getCreateTime();
                                    }
                                })
                );


        // 分组并开窗
        WindowedStream<Order, String, TimeWindow> windowedDs = orderDs.keyBy(Order::getUserId)
                // 每5秒通过过去5秒每个用户的订单总额
                .window(TumblingEventTimeWindows.of(Time.seconds(5)));

        OutputTag<Order> outputTag = new OutputTag<Order>("late_order_data_output_tag", TypeInformation.of(Order.class)) {
        };

        // 聚合统计，以及处理迟到数据
        // 在watermark设定延迟的基础上再多允许迟到2秒
        // watermark会先触发对应窗口的计算输出一次结果，但是不会关闭窗口，而是会再多允许迟到2秒（本例子意思就是最多允许迟到5秒），
        // 如果有属于该窗口区间的会继续进行聚合计算并输出结果
        // !!! 实际场景中需要控制好这两个设置，毕竟太多的话会导致延迟偏高
        SingleOutputStreamOperator<Tuple2<String, Double>> aggregateDs = windowedDs.allowedLateness(Time.seconds(2))
                // 在watermark设定的延迟和allowedLateness设定的延迟之后，还有迟到数据，通过次侧输出流输出
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<Order, Tuple2<String, Double>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Order> orderIter, Collector<Tuple2<String, Double>> out) throws Exception {
                        double sum = 0;
                        for (Order order : orderIter) {
                            sum += order.getMoney();
                        }
                        out.collect(Tuple2.of(key, sum));
                    }
                });

        // 打印正常的统计结果
        aggregateDs.print("normal");

        // 获取迟到数据并打印
        DataStream<Order> lateOrderDs = aggregateDs.getSideOutput(outputTag);
        lateOrderDs.printToErr("late");

        env.execute("D07_Window_EventTime_AllowedLateness");
    }
}
