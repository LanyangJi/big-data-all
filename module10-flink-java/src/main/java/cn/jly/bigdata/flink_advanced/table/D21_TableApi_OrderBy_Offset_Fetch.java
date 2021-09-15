package cn.jly.bigdata.flink_advanced.table;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author jilanyang
 * @createTime 2021/8/16 17:17
 */
public class D21_TableApi_OrderBy_Offset_Fetch {
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
        Table orderTable = tableEnv.fromDataStream(orderDS, $("orderId"), $("userId"), $("createTime").rowtime(), $("money"));

        /*
            oderBy
                类似于 SQL ORDER BY 子句。返回跨所有并行分区全局排序的记录。对于无界表，此操作需要对时间属性进行排序以及追加后续fetch操作。

                注意：orderBy必须是针对时间属性列，否则会报出： Sort on a non-time-attribute field is not supported.
         */
        Table orderByTable = orderTable.orderBy($("createTime").desc()).fetch(3);

        // 输出
        tableEnv.toRetractStream(orderByTable, Row.class).print();

        /*

            类似于 SQL OFFSET 和 FETCH 子句。偏移操作限制来自偏移位置的（可能已排序）结果。提取操作将（可能已排序的）结果限制为前 n 行。
            通常，这两个操作前面都有一个排序运算符。对于无界表，偏移操作需要获取操作。

            // returns the first 5 records from the sorted result
            Table result1 = in.orderBy($("a").asc()).fetch(5);

            // skips the first 3 records and returns all following records from the sorted result
            Table result2 = in.orderBy($("a").asc()).offset(3);

            // skips the first 10 records and returns the next 5 records from the sorted result
            Table result3 = in.orderBy($("a").asc()).offset(10).fetch(5);

         */


        env.execute("D21_TableApi_OrderBy_Offset_Fetch");
    }
}
