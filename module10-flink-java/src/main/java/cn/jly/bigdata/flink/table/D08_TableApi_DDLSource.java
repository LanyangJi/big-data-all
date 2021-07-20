package cn.jly.bigdata.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * connect的方式连接外部系统
 *
 * @author jilanyang
 * @date 2021/7/20 10:56
 * @package cn.jly.bigdata.flink.table
 * @class D08_TableApi_ConnectSource
 */
public class D08_TableApi_DDLSource {
    public static void main(String[] args) throws Exception {

        // 1. 创建表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. connect连接外部系统，这种方式正在flink正在重构中，下版本出新。当前老接口会报错->'format.type' expects 'csv', but is 'json'
        // flink官方也是推荐采用下面的DDL风格创建输入
//        tableEnv.connect(new FileSystem().path("d:/flink/source"))
//                // 解析json文件
//                .withFormat(new Json().deriveSchema())
//                .withSchema(
//                        // 顺序要和文件中读到的相一致
//                        new Schema().field("name", DataTypes.STRING())
//                                .field("temp", DataTypes.DOUBLE())
//                                .field("ts", DataTypes.BIGINT())
//                )
//                .createTemporaryTable("tbl_sensor");

        // 2. DDL方式连接外部系统创建出入
        String createSql = "create table tbl_sensor(`name` string, `temperature` double, `timestamp` bigint)" +
                " with('connector'='filesystem', 'path'='d:/flink/source', 'format'='json')";
        tableEnv.executeSql(createSql);

        // 3. table API操作
        Table sensorTable = tableEnv.from("tbl_sensor").as("name", "temp", "ts");
        Table queryTable = sensorTable.groupBy($("name")).select($("name"), $("temp").avg().as("avg_temp"));

        // 4. table转dataStream
        tableEnv.toChangelogStream(queryTable)
                .printToErr();

        env.execute("D08_TableApi_DDLSource");
    }
}
