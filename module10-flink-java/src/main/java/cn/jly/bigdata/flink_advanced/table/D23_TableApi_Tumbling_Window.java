package cn.jly.bigdata.flink_advanced.table;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 滚动窗口将行分配给固定长度的非重叠连续窗口。例如，一个 5 分钟的滚动窗口以 5 分钟的间隔对行进行分组。翻转窗口可以定义在事件时间、处理时间或行数上。
 * Tumbling windows are defined by using the Tumble class as follows:
 * <p>
 * Method	Description
 * over	    Defines the length the window, either as time or row-count interval.
 * on	    The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a declared event-time or processing-time time attribute.
 * as	    Assigns an alias to the window. The alias is used to reference the window in the following groupBy() clause and optionally to select window properties such as window start, end, or rowtime timestamps in the select() clause.
 *
 * @author jilanyang
 * @createTime 2021/8/18 11:05
 */
public class D23_TableApi_Tumbling_Window {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1); // 为了便于观察，这边并行度设置为1
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // source - 订单流
        SingleOutputStreamOperator<Order> orderDS = env.socketTextStream("localhost", 9999)
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

        /*
           开窗聚合
                // Tumbling Event-time Window 翻滚事件时间窗口
                .window(Tumble.over(lit(10).minutes()).on($("rowtime")).as("w"));

                // Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
                // 翻滚处理时间窗口（假设处理时间属性“proctime”）
                .window(Tumble.over(lit(10).minutes()).on($("proctime")).as("w"));

                // Tumbling Row-count Window (assuming a processing-time attribute "proctime")
                // Tumbling Row-count Window（假设处理时间属性为“proctime”）
                .window(Tumble.over(rowInterval(10)).on($("proctime")).as("w"));
         */
        Table aggTable = orderTable.window(
                Tumble.over(lit(5).second())
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

        aggTable.printSchema();

        // 输出
        DataStream<Tuple2<Boolean, AggResult>> resDS = tableEnv.toRetractStream(aggTable, AggResult.class);
        resDS.print();


        env.execute("D23_TableApi_Tumbling_Window");

    }

    @Data
    public static class AggResult {
        private String userId;
        private Timestamp w_start;
        private Timestamp w_end;
        private Double sum_money;
    }
}
