package cn.jly.bigdata.flink_advanced.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.ZoneId;

/**
 * 如果源中的时间戳数据表示为年-月-日-小时-分-秒，通常为不含时区信息的字符串值，例如2020-04-15 20:13:40.564，建议将事件时间属性定义为时间戳列：
 * <p>
 * +----------------+------------------------+------+-----+--------+-----------+
 * |           name |                   type | null | key | extras | watermark |
 * +----------------+------------------------+------+-----+--------+-----------+
 * |   window_start |           TIMESTAMP(3) | true |     |        |           |
 * |     window_end |           TIMESTAMP(3) | true |     |        |           |
 * | window_rowtime | TIMESTAMP(3) *ROWTIME* | true |     |        |           |
 * |           item |                 STRING | true |     |        |           |
 * |      max_price |                 DOUBLE | true |     |        |           |
 * +----------------+------------------------+------+-----+--------+-----------+
 * <p>
 * Use the following command to ingest data for MyTable2 in a terminal: ========== 输入的数据时间 ================
 * <p>
 * > nc -lk 9999
 * A,1.1,2021-04-15 14:01:00
 * B,1.2,2021-04-15 14:02:00
 * A,1.8,2021-04-15 14:03:00
 * B,2.5,2021-04-15 14:04:00
 * C,3.8,2021-04-15 14:05:00
 * C,3.8,2021-04-15 14:11:00
 * <p>
 * Flink SQL> SET 'table.local-time-zone' = 'UTC';          ####### 将本地时区设置为UTC
 * Flink SQL> SELECT * FROM MyView4;
 * <p>
 * ======================= 时间并没有改变 =========================
 * +-------------------------+-------------------------+-------------------------+------+-----------+
 * |            window_start |              window_end |          window_rowtime | item | max_price |
 * +-------------------------+-------------------------+-------------------------+------+-----------+
 * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    A |       1.8 |
 * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    B |       2.5 |
 * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    C |       3.8 |
 * +-------------------------+-------------------------+-------------------------+------+-----------+
 * <p>
 * Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';  ########### 将本地时区设置为上海
 * Flink SQL> SELECT * FROM MyView4;
 * Returns the same window start, window end and window rowtime compared to calculation in UTC timezone.
 * <p>
 * ======================== 时间依然没有改变 ============================
 * +-------------------------+-------------------------+-------------------------+------+-----------+
 * |            window_start |              window_end |          window_rowtime | item | max_price |
 * +-------------------------+-------------------------+-------------------------+------+-----------+
 * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    A |       1.8 |
 * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    B |       2.5 |
 * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    C |       3.8 |
 * +-------------------------+-------------------------+-------------------------+------+-----------+
 * <p>
 * 结论：
 * 无论设置以下哪种本地时区，本示例的输出结果都是一致的，因为指定的createTime事件时间属性为timestamp，不携带时区的概念
 *
 * @author jilanyang
 * @createTime 2021/8/10 16:03
 */
public class D08_TableApi_Sql_EventTimeAttributeOnTimestamp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // !!!!!!!!!! 注意 ！！！！！！！！！！！！！！
        // 此时无论设置以下哪种本地时区，本示例的输出结果都是一致的，因为指定的createTime事件时间属性为timestamp，不携带时区的概念
        //tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        // 创建输入表
        tableEnv.executeSql(
                "create table if not exists tbl_order(" +
                        " item string," +
                        " price double," +
                        " createTime timestamp(3)," +
                        " watermark for createTime as createTime - interval '3' second" + // 允许3秒延迟的水印
                        " ) with (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 'test'," +
                        "  'properties.bootstrap.servers' = 'linux01:9092'," +
                        "  'properties.group.id' = 'testGroup'," +
                        "  'scan.startup.mode' = 'latest-offset'," +
                        "  'format' = 'csv'" +
                        " )"
        );

        tableEnv.from("tbl_order").printSchema();

        // 创建统计表（中间结果表）
        tableEnv.executeSql(
                "create view myView as" +
                        " select" +
                        "    tumble_start(createTime, interval '5' second) as window_start," +
                        "    tumble_end(createTime, interval '5' second) as window_end," +
                        "    tumble_rowtime(createTime, interval '5' second) as window_rowtime," +
                        "    item," +
                        "    max(price) as max_price" +
                        "  from tbl_order" +
                        "    group by tumble(createTime, interval '5' second), item"
        );

        Table table = tableEnv.sqlQuery("select * from myView");
        table.printSchema();

        // 转换为流输出
        DataStream<Tuple2<Boolean, Row>> resDS = tableEnv.toRetractStream(table, Row.class);
        resDS.printToErr();

        env.execute("D08_TableApi_Sql_EventTimeAttributeOnTimestamp");
    }
}
