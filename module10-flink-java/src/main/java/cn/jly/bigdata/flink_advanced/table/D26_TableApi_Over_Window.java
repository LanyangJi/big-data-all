package cn.jly.bigdata.flink_advanced.table;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Over window 聚合是从标准 SQL（OVER 子句）中得知的，并在查询的 SELECT 子句中定义。
 * 与在 GROUP BY 子句中指定的组窗口不同，窗口上方不会折叠行。相反，在窗口聚合上为每个输入行在其相邻行的范围内计算聚合。
 *
 * OverWindow 定义了计算聚合的行范围。 OverWindow 不是用户可以实现的接口。相反，Table API 提供了 Over 类来配置覆盖窗口的属性。
 * 可以在事件时间或处理时间以及指定为时间间隔或行计数的范围内定义窗口。支持的窗口定义作为 Over（和其他类）上的方法公开，并在下面列出：
 *
 * partition by(可选):
 * 在一个或多个属性上定义输入的分区。每个分区都单独排序，聚合函数分别应用于每个分区。
 * 注意：在流环境中，如果窗口包含 partition by 子句，则只能并行计算窗口聚合。如果没有 partitionBy(…)，流由单个非并行任务处理。
 *
 *
 *
 * @author jilanyang
 * @createTime 2021/8/19 11:26
 */
public class D26_TableApi_Over_Window {
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

        // over window


        env.execute("D26_TableApi_Over_Window");
    }
}
