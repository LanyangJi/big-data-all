package cn.jly.bigdata.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author jilanyang
 * @date 2021/7/21 10:44
 */
public class D11_TableApi_Kafka {
    public static void main(String[] args) throws Exception {
        // 1. 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 连接kafka - 输入表
        String createKafkaInputTableSql =
                "create table kafkaTable(`name` string, `temperature` double, `timestamp` bigint, `ts` timestamp(3) metadata from 'timestamp')" +
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
        Table queryTable = kafkaTable.groupBy($("name")).select($("name"), $("temp").avg().as("avg_temp"));

        // 4. sink到kafka
        queryTable.executeInsert("kafkaOutputTable");

        env.execute("D11_TableApi_Kafka");
    }
}
