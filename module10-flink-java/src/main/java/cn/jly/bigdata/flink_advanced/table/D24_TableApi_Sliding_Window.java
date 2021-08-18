package cn.jly.bigdata.flink_advanced.table;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 滑动窗口具有固定大小并按指定的滑动间隔滑动。如果滑动间隔小于窗口大小，则滑动窗口重叠。
 * 因此，可以将行分配给多个窗口。例如，15 分钟大小和 5 分钟滑动间隔的滑动窗口将每一行分配给 3 个不同的 15 分钟大小的窗口，
 * 以 5 分钟的间隔进行评估。滑动窗口可以在事件时间、处理时间或行数上定义。
 * <p>
 * Sliding windows are defined by using the Slide class as follows:
 * <p>
 * Method	Description
 * over	    Defines the length of the window, either as time or row-count interval.
 * every	Defines the slide interval, either as time or row-count interval. The slide interval must be of the same type as the size interval.
 * on	    The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a declared event-time or processing-time time attribute.
 * as	    Assigns an alias to the window. The alias is used to reference the window in the following groupBy() clause and optionally to select window properties such as window start, end, or rowtime timestamps in the select() clause.
 * <p>
 * <p>
 * // Sliding Event-time Window
 * .window(Slide.over(lit(10).minutes())
 * .every(lit(5).minutes())
 * .on($("rowtime"))
 * .as("w"));
 * <p>
 * <p>
 * // Sliding Processing-time window (assuming a processing-time attribute "proctime")
 * .window(Slide.over(lit(10).minutes())
 * .every(lit(5).minutes())
 * .on($("proctime"))
 * .as("w"));
 * <p>
 * <p>
 * // Sliding Row-count window (assuming a processing-time attribute "proctime")
 * .window(Slide.over(rowInterval(10)).every(rowInterval(5)).on($("proctime")).as("w"));
 *
 * @author jilanyang
 * @createTime 2021/8/18 17:14
 */
public class D24_TableApi_Sliding_Window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // source - 订单流
        SingleOutputStreamOperator<Order> orderDS = env.socketTextStream("linux01", 9999)
                .flatMap(
                        new FlatMapFunction<String, Order>() {
                            @Override
                            public void flatMap(String s, Collector<Order> collector) throws Exception {
                                String[] fields = s.split(",");
                                String orderId = fields[0];
                                String userId = fields[1];
                                long createTime = Long.parseLong(fields[2]);
                                double money = Double.parseDouble(fields[3]);

                                collector.collect(new Order(orderId, userId, createTime, money));
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        // 分配时间戳和水印，允许3秒的延迟
                        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Order>() {
                                            @Override
                                            public long extractTimestamp(Order order, long l) {
                                                return order.getCreateTime();
                                            }
                                        }
                                )
                );

        // dataStream -> table
        Table orderTable = tableEnv.fromDataStream(
                orderDS,
                $("orderId"),
                $("userId"),
                $("createTime").rowtime(),
                $("money")
        );

        // 滑动窗口聚合：每5秒钟计算过去15秒的每个用户的订单总额
        Table slidingAggTable = orderTable.window(
                Slide.over(lit(15).second())
                        .every(lit(5).second())
                        .on($("createTime"))
                        .as("w")
        )
                .groupBy($("w"), $("userId"))
                .select(
                        $("userId"),
                        $("w").start().as("w_start"),
                        $("w").end().as("w_end"),
                        $("money").sum().as("sum_money")
                );

        // 输出
        tableEnv.toRetractStream(slidingAggTable, Row.class).print();

        env.execute("D24_TableApi_Sliding_Window");
    }
}
