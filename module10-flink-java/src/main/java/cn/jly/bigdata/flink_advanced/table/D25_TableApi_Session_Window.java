package cn.jly.bigdata.flink_advanced.table;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 会话窗口没有固定大小，但它们的边界由不活动的间隔定义，即，如果在定义的间隙期间没有事件出现，会话窗口将关闭。
 * 例如，当在 30 分钟不活动后观察到一行时，会有 30 分钟间隔的会话窗口开始（否则该行将被添加到现有窗口中），
 * 如果在 30 分钟内没有添加行，则关闭。会话窗口可以在事件时间或处理时间工作。
 * <p>
 * A session window is defined by using the Session class as follows:
 * <p>
 * Method	Description
 * withGap	Defines the gap between two windows as time interval.
 * on	    The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a declared event-time or processing-time time attribute.
 * as	    Assigns an alias to the window. The alias is used to reference the window in the following groupBy() clause and optionally to select window properties such as window start, end, or rowtime timestamps in the select() clause.
 * <p>
 * // Session Event-time Window
 * .window(Session.withGap(lit(10).minutes()).on($("rowtime")).as("w"));
 * <p>
 * // Session Processing-time Window (assuming a processing-time attribute "proctime")
 * .window(Session.withGap(lit(10).minutes()).on($("proctime")).as("w"));
 *
 * @author jilanyang
 * @createTime 2021/8/18 17:28
 */
public class D25_TableApi_Session_Window {
    @SneakyThrows
    public static void main(String[] args) {
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

        // session window
        Table sessionAggTable = orderTable.window(
                Session.withGap(lit(5).second())
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
        tableEnv.toRetractStream(sessionAggTable, Row.class).print();

        env.execute("D25_TableApi_Session_Window");
    }
}
