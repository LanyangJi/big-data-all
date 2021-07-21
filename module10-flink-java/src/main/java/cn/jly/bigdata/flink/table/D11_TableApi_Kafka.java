package cn.jly.bigdata.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author jilanyang
 * @date 2021/7/21 10:44
 */
public class D11_TableApi_Kafka {
    public static void main(String[] args) throws Exception {
        // 1. 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 连接kafka
        String createKafkaTableSql =
                "create table kafkaTable(`name` string, `temperature` double, `event_time` bigint, `ts` timestamp(3) metadata from 'timestamp')" +
                        " with('connector' = 'kafka', 'topic' = 'sensor_reading', 'properties.bootstrap.servers' = 'linux01:9092'," +
                        " 'properties.group.id' = 'testGroup', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json')";
        tableEnv.executeSql(createKafkaTableSql);

        // 打印输出
        Table kafkaTable = tableEnv.from("kafkaTable").as("name", "temp", "event_time", "ts");
        kafkaTable.printSchema();

        tableEnv.toRetractStream(kafkaTable, Row.class).print();

        env.execute("D11_TableApi_Kafka");
    }
}
