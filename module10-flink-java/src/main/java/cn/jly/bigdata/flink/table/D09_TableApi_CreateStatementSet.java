package cn.jly.bigdata.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author jilanyang
 * @date 2021/7/20 15:52
 */
public class D09_TableApi_CreateStatementSet {
    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 执行建表语句
        // 输入表
        String createInputTableSql = "create table tbl_sensor(`name` string, `temperature` double, `timestamp` bigint)" +
                " with('connector'='filesystem', 'path'='d:/flink/source', 'format'='json')";
        tableEnv.executeSql(createInputTableSql);

        // 输出表1
        String createOutputTableSql1 = "create table tbl_sensor_out1(`name` string, `temperature` double, `timestamp` bigint)" +
                " with('connector'='filesystem', 'path'='d:/flink/multi_sink1', 'format'='json')";
        tableEnv.executeSql(createOutputTableSql1);
        // 输出表2
        String createOutputTableSql2 = "create table tbl_sensor_out2(`name` string, `temperature` double, `timestamp` bigint)" +
                " with('connector'='filesystem', 'path'='d:/flink/multi_sink2', 'format'='json')";
        tableEnv.executeSql(createOutputTableSql2);
        // 输出表3
        String createOutputTableSql3 = "create table tbl_sensor_out3(`name` string, `avg_temp` double)" +
                " with('connector'='filesystem', 'path'='d:/flink/multi_sink3', 'format'='json')";
        tableEnv.executeSql(createOutputTableSql3);
        // 输出表4
        String createOutputTableSql4 = "create table tbl_sensor_out4(`name` string, `avg_temp` double)" +
                " with('connector'='filesystem', 'path'='d:/flink/multi_sink4', 'format'='json')";
        tableEnv.executeSql(createOutputTableSql4);
        // 输出表5
        String createOutputTableSql5 = "create table tbl_sensor_out5(`name` string, `temperature` double, `timestamp` bigint)" +
                " with('connector'='filesystem', 'path'='d:/flink/multi_sink5', 'format'='json')";
        tableEnv.executeSql(createOutputTableSql5);
        // 输出表6
        String createOutputTableSql6 = "create table tbl_sensor_out6(`name` string, `temperature` double, `timestamp` bigint)" +
                " with('connector'='filesystem', 'path'='d:/flink/multi_sink6', 'format'='json')";
        tableEnv.executeSql(createOutputTableSql6);

        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // 3. 单sink和StatementSet执行多sink
        // 单sink
        tableEnv.from("tbl_sensor").executeInsert("tbl_sensor_out1"); // table API方式
        tableEnv.executeSql("insert into tbl_sensor_out2 select * from tbl_sensor"); // sql

        // 多sink
        // addInert和addInsertSql不支持更新操作，只能insert: Table sink 'default_catalog.default_database.tbl_sensor_out3' doesn't support consuming update changes which is produced by node GroupAggregate(groupBy=[name], select=[name, AVG(temperature) AS avg_temp])
//        Table outputTable3 = tableEnv.from("tbl_sensor").groupBy($("name")).select($("name"), $("temperature").avg().as("avg_temp"));
//        Table outputTable4 = tableEnv.from("tbl_sensor").groupBy($("name")).select($("name"), $("temperature").avg().as("avg_temp"));
//        tableEnv.createStatementSet()
//                .addInsert("tbl_sensor_out3", outputTable3)
//                .addInsert("tbl_sensor_out4", outputTable4)
//                .execute();

//        tableEnv.createStatementSet()
//                .addInsertSql("insert into tbl_sensor_out3 select name, avg(temperature) as avg_temp from tbl_sensor group by name")
//                .addInsertSql("insert into tbl_sensor_out4 select name, avg(temperature) as avg_temp from tbl_sensor group by name")
//                .execute();

        // 紧插入方式是支持的
        tableEnv.createStatementSet()
                .addInsert("tbl_sensor_out5", tableEnv.from("tbl_sensor"))
                .addInsert("tbl_sensor_out6", tableEnv.from("tbl_sensor"))
                .execute();

        tableEnv.createStatementSet()
                .addInsertSql("insert into tbl_sensor_out5 select * from tbl_sensor")
                .addInsertSql("insert into tbl_sensor_out6 select * from tbl_sensor")
                .execute();

        // 隐式sink
        tableEnv.from("tbl_sensor").execute().print(); // 报错但不影响结果：MiniCluster is not yet running or has already been shut down.
        tableEnv.executeSql("select * from tbl_sensor").print();

        // env.execute("D09_TableApi_CreateStatementSet");
    }
}
