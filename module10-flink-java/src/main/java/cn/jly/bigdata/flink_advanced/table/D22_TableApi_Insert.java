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
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * !!!!!!!!!! sql 中不允许有分号 ！！！！！！！！！！！！！！！！！！
 * <p>
 * 类似于 SQL 查询中的 INSERT INTO 子句，该方法执行插入到已注册输出表中的操作。 executeInsert() 方法将立即提交执行插入操作的 Flink 作业。
 * 输出表必须在 TableEnvironment 中注册（参见连接器表）。此外，输入表与输出表的schema必须相匹配。
 *
 * @author jilanyang
 * @createTime 2021/8/18 10:31
 */
public class D22_TableApi_Insert {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1); // 为了方便查看
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

        // 输入表
        Table orderTable = tableEnv.fromDataStream(orderDS, $("orderId"), $("userId"), $("createTime").rowtime(), $("money"));
        tableEnv.createTemporaryView("tbl_order", orderTable);

        // 输出表
        tableEnv.executeSql(
                "create table if not exists fs_order_table ( " +
                        "    orderId string, " +
                        "    userId string, " +
                        "    createTime timestamp(3), " +
                        "    money double, " +
                        "    dt string, " +         // 一级分区字段：日期
                        "    `hour` string " +      // 二级分区字段：小时
                        ") partitioned by (dt, `hour`) with ( " +
                        "    'connector' = 'filesystem', " +
                        "    'path' = 'file:///D:/test/', " +
                        "    'format' = 'json', " +
                        "    'sink.partition-commit.delay'='1 h', " +
                        "    'sink.partition-commit.policy.kind'='success-file' " +
                        ")"
        );

        // 方式一：以sql的方式插入
        tableEnv.executeSql(
                "insert into fs_order_table  " +
                        "select  " +
                        "      orderId,  " +
                        "      userId,  " +
                        "      createTime,  " +
                        "      money,  " +
                        "      date_format(createTime, 'yyyy-MM-dd'),  " +
                        "      date_format(createTime, 'HH')  " +
                        "from tbl_order"
        );

        // 方式二: tableApi的方式
        // 但是本示例不合适，因为两个表的列并不匹配，输出表中本示例添加了2个分区字段，需要从输入表的时间字段中转换计算而来
        // orderTable.executeInsert("fs_order_table")

        env.execute("D22_TableApi_Insert");
    }
}
