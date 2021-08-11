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
 * 如果源中的时间戳数据表示为新纪元时间，通常为long类型的值，例如1618989564564，建议将事件时间属性定义为timestamp_ltz列。携带本地时区
 * <p>
 * The input data of MyTable3 is: ================== 输入的时间 =======================
 * <p>
 * A,1.1,1618495260000  # The corresponding utc timestamp is 2021-04-15 14:01:00
 * B,1.2,1618495320000  # The corresponding utc timestamp is 2021-04-15 14:02:00
 * A,1.8,1618495380000  # The corresponding utc timestamp is 2021-04-15 14:03:00
 * B,2.5,1618495440000  # The corresponding utc timestamp is 2021-04-15 14:04:00
 * C,3.8,1618495500000  # The corresponding utc timestamp is 2021-04-15 14:05:00
 * C,3.8,1618495860000  # The corresponding utc timestamp is 2021-04-15 14:11:00
 * <p>
 * Flink SQL> SET 'table.local-time-zone' = 'UTC';           ##### 将本地时区设置为UTC
 * Flink SQL> SELECT * FROM MyView5;
 * <p>
 * ======================== 发现时间并没有变化，说明默认是按照UTC时间处理 ==============================
 * +-------------------------+-------------------------+-------------------------+------+-----------+
 * |            window_start |              window_end |          window_rowtime | item | max_price |
 * +-------------------------+-------------------------+-------------------------+------+-----------+
 * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    A |       1.8 |
 * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    B |       2.5 |
 * | 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    C |       3.8 |
 * +-------------------------+-------------------------+-------------------------+------+-----------+
 * <p>
 * Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';  ######### 将本地时区转换为上海
 * Flink SQL> SELECT * FROM MyView5;
 * Returns the different window start, window end and window rowtime compared to calculation in UTC timezone.
 * <p>
 * ===========================   发现时间被加上了8个小时 ==========================================
 * +-------------------------+-------------------------+-------------------------+------+-----------+
 * |            window_start |              window_end |          window_rowtime | item | max_price |
 * +-------------------------+-------------------------+-------------------------+------+-----------+
 * | 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    A |       1.8 |
 * | 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    B |       2.5 |
 * | 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    C |       3.8 |
 * +-------------------------+-------------------------+-------------------------+------+-----------+
 *
 * @author jilanyang
 * @createTime 2021/8/10 16:03
 */
public class D09_TableApi_Sql_EventTimeAttributeOnTimestampLTZ {
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
                        " createTime bigint," + // 以新纪元毫秒为单位的long类型时间戳
                        " createTime_ltz as to_timestamp_ltz(createTime, 3)," + // 调用to_timestamp_ltz方法将long时间戳转换为精度为3（毫秒）的timestamp_ltz
                        " watermark for createTime_ltz as createTime_ltz - interval '3' second" + // 允许3秒延迟的水印
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
                        "    tumble_start(createTime_ltz, interval '5' second) as window_start," + // 本地时区
                        "    tumble_end(createTime_ltz, interval '5' second) as window_end," +  // 本地时区
                        "    tumble_rowtime(createTime_ltz, interval '5' second) as window_rowtime," + // 按照utc
                        "    item," +
                        "    max(price) as max_price" +
                        "  from tbl_order" +
                        "    group by tumble(createTime_ltz, interval '5' second), item"
        );

        Table table = tableEnv.sqlQuery("select * from myView");
        table.printSchema();

        // 转换为流输出
        DataStream<Tuple2<Boolean, Row>> resDS = tableEnv.toRetractStream(table, Row.class);
        resDS.printToErr();

        env.execute("D09_TableApi_Sql_EventTimeAttributeOnTimestampLTZ");
    }
}
