package cn.jly.bigdata.flink_advanced.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.ZoneId;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author jilanyang
 * @createTime 2021/8/12 9:21
 */
public class D10_TableApi_Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1); // 为了方便看测试结果

        // 创建流表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设置本地时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        System.out.println("系统默认时区为：" + ZoneId.systemDefault());

        // 创建输入表
        String createInputTableSql =
                "create table if not exists tbl_order (" +
                        "    orderId string," +
                        "    userId string," +
                        "    money double," +
                        "    createTime bigint," +
                        "    createTime_ltz as to_timestamp_ltz(createTime, 3)," + // 打印输出的是UTC统一时间的0时区
                        "    watermark for createTime_ltz as createTime_ltz - interval '3' second" +
                        ") with (" +
                        "    'connector' = 'kafka'," +
                        "    'topic' = 'test'," +
                        "    'properties.bootstrap.servers' = 'linux01:9092'," +
                        "    'properties.group.id' = 'myTestGroup'," +
                        "    'scan.startup.mode' = 'latest-offset'," +
                        "    'format' = 'json'" +
                        ")";
        tableEnv.executeSql(createInputTableSql);

        // 打印表的结构
        Table orderTable = tableEnv.from("tbl_order");
        orderTable.printSchema();

        // 转换为流输出
        DataStream<Row> resDS = tableEnv.toAppendStream(orderTable, Row.class);
        resDS.print();

        // table api操作
        Table resTable = orderTable
                .filter(
                        and(
                                $("orderId").isNotNull(),
                                $("userId").isNotNull(),
                                $("money").isNotNull()
                        )
                )
                .select(
                        $("userId").lowerCase().as("userId"),
                        $("money"),
                        $("createTime_ltz") // 水印可以向下传递
                )
                .window(
                        Tumble.over(lit(5).second()).on($("createTime_ltz")).as("w")
                )
                .groupBy(
                        $("w"), $("userId")
                )
                .select(
                        $("userId"),
                        $("w").start().as("window_start"),
                        $("w").end().as("window_end"),
                        $("money").max().as("max_money")
                );
        // 转换为DS并打印输出
        DataStream<Row> resDS2 = tableEnv.toAppendStream(resTable, Row.class);
        resDS2.printToErr();

        env.execute("D10_TableApi_Example");
    }
}
