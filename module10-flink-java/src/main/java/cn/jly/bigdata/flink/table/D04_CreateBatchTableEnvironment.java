package cn.jly.bigdata.flink.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.descriptors.ConnectorDescriptor;

import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;

/**
 * flink处理json需要引入
 * <dependency>
 * <groupId>org.apache.flink</groupId>
 * <artifactId>flink-json</artifactId>
 * <version>${flink.version}</version>
 * </dependency>
 *
 * @author jilanyang
 * @date 2021年7月15日22:42:56
 */
public class D04_CreateBatchTableEnvironment {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        // 创建批处理表环境
        TableEnvironment batchTableEnv = TableEnvironment.create(envSettings);

        // 创建输入表
        String inputCreateSql = "create table tbl_sensor(name string, temperature double, `timestamp` bigint)" +
                " with('connector'='filesystem', 'path'='input/sensorReading.json', 'format'='json')";
        batchTableEnv.executeSql(inputCreateSql);

        Table inputTable = batchTableEnv.from("tbl_sensor");
        inputTable.printSchema();

        // 创建输出表
        String outputCreateSql = "create table tbl_sensor_out(name string, avg_temp double)" +
                " with('connector'='filesystem', 'path'='output/sensorReading_out.json', 'format'='json')";
        batchTableEnv.executeSql(outputCreateSql);

        // table api
        Table groupTable = inputTable.groupBy($("name")).select($("name"), $("temperature").avg().as("avg_temp"));

        // 输出到输出表
        groupTable.executeInsert("tbl_sensor_out");
    }
}
