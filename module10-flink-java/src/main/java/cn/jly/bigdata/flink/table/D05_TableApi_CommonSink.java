package cn.jly.bigdata.flink.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author jilanyang
 * @date 2021/7/19 16:11
 */
public class D05_TableApi_CommonSink {
    public static void main(String[] args) {
        // 1. 创建批处理表环境
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        TableEnvironment batchTableEnv = TableEnvironment.create(envSettings);

        // 2. 声明输入表
        String inputCreateSql = "create table tbl_sensor(name string, temperature double, `timestamp` bigint)" +
                " with('connector'='filesystem', 'path'='input/sensorReading.json', 'format'='json')";
        batchTableEnv.executeSql(inputCreateSql);

        // 3. 拿到inputTable对象
        Table inputTable = batchTableEnv.from("tbl_sensor");
        inputTable.printSchema();

        // 4. table api操作
        Table queryTable = inputTable.groupBy($("name")).select($("name"), $("temperature").avg().as("avg_temp"));

        // 5. 创建输出表 -- tableSink
        String outputTableName = "tbl_csvSinkTable";
        Schema tableSchema = new Schema()
                .field("sensor_name", DataTypes.STRING())
                .field("avg_temp", DataTypes.DOUBLE());
        // connect方式将在下个版本重构，推荐使用DDL风格(create table...)
        batchTableEnv.connect(new FileSystem().path("d:/flink/table_sink"))
                .withFormat(new Csv().fieldDelimiter('|').deriveSchema())
                .withSchema(tableSchema)
                .createTemporaryTable(outputTableName);

        // 6. 将结果插入表
        queryTable.executeInsert(outputTableName);
    }
}
