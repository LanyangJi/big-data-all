package cn.jly.bigdata.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 链接
 * https://www.cnblogs.com/wynjauu/articles/11654838.html
 * <p>
 * 使用flinkSQL处理实时数据当我们把表转化成流的时候，需要使用toAppendStream与toRetractStream这两个方法。稍不注意可能直接选择了toAppendStream。这个两个方法还是有很大区别的，下面具体介绍。
 * <p>
 * toAppendStream与toRetractStream的区别
 * 追加模式：只有在动态Table仅通过INSERT更改修改时才能使用此模式，即它仅附加，并且以前发出的结果永远不会更新。
 * <p>
 * 如果更新或删除操作使用追加模式会失败报错
 * <p>
 * 缩进模式：始终可以使用此模式。返回值是boolean类型。它用true或false来标记数据的插入和撤回，返回true代表数据插入，false代表数据的撤回
 * <p>
 * 按照官网的理解如果数据只是不断添加，可以使用追加模式，其余方式则不可以使用追加模式，而缩进模式侧可以适用于更新，删除等场景。
 * <p>
 * Table API更新模式
 * 1. 仅插入模式 Append
 * 2. 撤回模式 Retract
 * 3. 更新插入模式 Upsert
 *
 * @author jilanyang
 * @date 2021/7/21 10:44
 */
public class D11_TableApi_Kafka {
    public static void main(String[] args) throws Exception {
        // 1. 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 连接kafka
        String createKafkaInputTableSql =
                "create table kafkaTable(`name` string, `temperature` double, `event_time` bigint, `ts` timestamp(3) metadata from 'timestamp')" +
                        " with('connector' = 'kafka', 'topic' = 'sensor_reading', 'properties.bootstrap.servers' = 'linux01:9092'," +
                        " 'properties.group.id' = 'testGroup', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json')";
        tableEnv.executeSql(createKafkaInputTableSql);

        // 连接kafka - 输出表
        String createKafkaOutputTableSql =
                "create table kafkaOutputTable(`name` string, `avg_temp` double)" +
                        " with('connector' = 'kafka', 'topic' = 'sensor_reading_out', " +
                        "'properties.bootstrap.servers' = 'linux01:9092','format' = 'json')";
        tableEnv.executeSql(createKafkaOutputTableSql);

        // 打印输出
        Table kafkaTable = tableEnv.from("kafkaTable").as("name", "temp", "event_time", "ts");
        kafkaTable.printSchema();

        // 3. table api数据转换
        Table aggregateTable = kafkaTable.groupBy($("name")).select($("name"), $("temp").avg().as("avg_temp"));
        Table queryTable = kafkaTable.select($("name"), $("temp").as("avg_temp"));


        // 4. sink到kafka  -- 会报错，因为kafka sink目前只支持append模式（Insert,Append），即追加模式，不支持修改（缩进模式）
        // aggregateTable.executeInsert("kafkaOutputTable");
        queryTable.executeInsert("kafkaOutputTable");

        // env.execute("D11_TableApi_Kafka");
    }
}
