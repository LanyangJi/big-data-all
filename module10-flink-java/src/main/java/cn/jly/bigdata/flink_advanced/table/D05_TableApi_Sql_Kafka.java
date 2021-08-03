package cn.jly.bigdata.flink_advanced.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 从Kafka中消费数据并过滤出状态为success的数据再写入到Kafka
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.table
 * @class D05_TableApi_Sql_Kafka
 * @date 2021/8/2 22:52
 */
public class D05_TableApi_Sql_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 建kafka source表
        String InputKafkaSql = "CREATE TABLE input_kafka (" +
                "  `user_id` BIGINT," +
                "  `page_id` BIGINT," +
                "  `status` STRING," +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'input_kafka'," +
                "  'properties.bootstrap.servers' = 'linux01:9092'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'json'" +
                ")";
        tableEnv.executeSql(InputKafkaSql);

        // 建kafka sink表
        String outputKafkaSql = "CREATE TABLE output_kafka (" +
                "  `user_id` BIGINT," +
                "  `page_id` BIGINT," +
                "  `status` STRING," +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'output_kafka'," +
                "  'properties.bootstrap.servers' = 'linux01:9092'," +
                "  'format' = 'json'," +
                "  'sink.partitioner' = 'round-robin'" +
                ")";
        tableEnv.executeSql(outputKafkaSql);

        // 过滤
        Table queryTable = tableEnv.from("input_kafka")
                .filter($("status").isEqual("SUCCESS"));

        // Table table = tableEnv.sqlQuery("select * from input_kafka where `status`='SUCCESS'");
        // tableEnv.executeSql("insert into output_kafka select * from " + table);

        // 输出
        queryTable.executeInsert("output_kafka");

        // env.execute("D05_TableApi_Sql_Kafka");
    }
}
