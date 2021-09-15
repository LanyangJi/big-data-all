package cn.jly.bigdata.flink_advanced.table;

import cn.jly.bigdata.flink_advanced.datastream.beans.Customer;
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

import static org.apache.flink.table.api.Expressions.*;

/**
 * 类似于 SQL JOIN 子句。连接两个表。两个表必须具有不同的字段名称，并且必须通过连接运算符或使用 where 或过滤器运算符定义至少一个相等连接谓词。
 *
 * @author jilanyang
 * @createTime 2021/8/13 16:06
 */
public class D19_TableApi_Join {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1); // 为了方便查看，这边并行度设置为1
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
                        // 允许3秒的延迟
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

        // 用户信息流
        SingleOutputStreamOperator<Customer> customerDS = env.socketTextStream("linux01", 8888).flatMap(
                new FlatMapFunction<String, Customer>() {
                    @Override
                    public void flatMap(String s, Collector<Customer> collector) throws Exception {
                        String[] fields = s.split(",");
                        collector.collect(new Customer(fields[0], fields[1], Integer.parseInt(fields[2]), Long.parseLong(fields[3])));
                    }
                }
        )
                .assignTimestampsAndWatermarks(
                        // 允许3秒的延迟
                        WatermarkStrategy.<Customer>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Customer>() {
                                            @Override
                                            public long extractTimestamp(Customer customer, long l) {
                                                return customer.getCreateTime();
                                            }
                                        }
                                )
                );

        // DataStream -> Table
        Table orderTable = tableEnv.fromDataStream(orderDS, $("orderId"), $("userId"), $("createTime"), $("money"));
        Table customerTable = tableEnv.fromDataStream(customerDS, $("id"), $("name"), $("age"));

        // inner join
        Table queryTable = orderTable.join(customerTable)
                .where($("userId").isEqual($("id"))) // 两个表必须具有不同的字段名称
                .select($("userId"), $("name"), $("age"), $("money"));

        // 输出
        tableEnv.toAppendStream(queryTable, Row.class).print("inner join");

        // leftOuterJoin
        // 左边来数据的时候，右边如果没有，则会以null补充
        // 等右边来数据的时候，撤回上一条以null补充的数据，重新发送join好的数据
        Table leftOuterJoinTable = orderTable.leftOuterJoin(customerTable, $("userId").isEqual($("id"))) // 两个表必须具有不同的字段名称
                .select($("userId"), $("name"), $("age"), $("money"));

        tableEnv.toRetractStream(leftOuterJoinTable, Row.class).print("leftOuterJoinTable");

        // rightOuterJoin
        // 道理与左外连接类似
        Table rightOuterJoinTable = orderTable.rightOuterJoin(customerTable, $("userId").isEqual($("id"))) // 两个表必须具有不同的字段名称
                .select($("userId"), $("name"), $("age"), $("money"));

        tableEnv.toRetractStream(rightOuterJoinTable, Row.class).print("rightOuterJoinTable");

        // fullOuterJoin
        // 道理与左外连接类似
        Table fullOuterJoinTable = orderTable.fullOuterJoin(customerTable, $("userId").isEqual($("id"))) // 两个表必须具有不同的字段名称
                .select($("userId"), $("name"), $("age"), $("money"));

        tableEnv.toRetractStream(fullOuterJoinTable, Row.class).print("fullOuterJoinTable");

        /*
            间隔连接至少需要一个等值连接谓词和一个限制双方时间的连接条件。
            这种条件可以由两个适当的范围谓词（<、<=、>=、>）或一个比较两个输入表的相同类型（即处理时间或事件时间）的时间属性的等式谓词来定义。
         */
        Table orderTableWithTs = tableEnv.fromDataStream(
                orderDS,
                $("orderId"),
                $("userId"),
                $("money"),
                $("createTime").rowtime().as("o_time")
        );
        orderTableWithTs.printSchema();

        Table customerTableWithTs = tableEnv.fromDataStream(
                customerDS,
                $("id"),
                $("name"),
                $("age"),
                $("createTime").rowtime().as("c_time")
        );
        customerTableWithTs.printSchema();

        Table intervalJoinTable = orderTableWithTs.join(customerTableWithTs)
                .where(
                        and(
                                $("userId").isEqual($("id")), // 间隔连接至少需要一个等值连接谓词和一个限制双方时间的连接条件
                                $("o_time").isGreaterOrEqual($("c_time").minus(lit(5).seconds())), // c_time - 5 <= o_time <= c_time + 5
                                $("o_time").isLessOrEqual($("c_time").plus(lit(5).seconds()))
                        )
                )
                .select(
                        $("orderId"),
                        $("userId"),
                        $("name"),
                        $("o_time")
                        // $("c_time") // 只能由一个rowTime被转换为DataStream
                );
        // 输出
        tableEnv.toRetractStream(intervalJoinTable, Row.class).printToErr("intervalJoinTable");

        // todo 各种join结合UDTF的方式等研究过Flink UDTF再来添加

        env.execute("D19_TableApi_Join");
    }
}
