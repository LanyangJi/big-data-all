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
