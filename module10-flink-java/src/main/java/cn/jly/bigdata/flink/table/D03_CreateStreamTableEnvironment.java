package cn.jly.bigdata.flink.table;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 动态表的概念
 * 流数据的每一条记录都是动态表的新的行
 * 官方说法：为了使用关系查询处理流，必须将其转换成 Table。从概念上讲，流的每条记录都被解释为对结果表的 INSERT 操作。
 * 本质上我们正在从一个 INSERT-only 的 changelog 流构建表。
 *
 * @author lanyangji
 * @date 2021/7/8 20:05
 * @packageName cn.jly.bigdata.flink.table
 * @className D01_CreateStreamTableEnvironment
 */
public class D03_CreateStreamTableEnvironment {
    public static void main(String[] args) throws Exception {
        // 方式一：
        // 环境设置对象
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .inStreamingMode()
//                .build();

        // table environment
//        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 方式二
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
        int port = tool.getInt("port", 9999);

        // dataStream
        DataStreamSource<String> inputDataStream = env.socketTextStream(host, port);
        SingleOutputStreamOperator<SensorReading> sensorReadingDataStream = inputDataStream.map(
                new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String value) throws Exception {
                        return JSON.parseObject(value, SensorReading.class);
                    }
                }
        );

        // dataStream -> table
        // as修改table的列名，可选操作。不指定的话按照SensorReading属性名为列名
        Table table = tableEnv.fromDataStream(sensorReadingDataStream).as("name", "ts", "temp");

        // 创建视图view
        // 方法一：通过dataStream直接创建
        // tableEnv.createTemporaryView("sensorReading", sensorReadingDataStream);
        // 方法二：通过table创建
        tableEnv.createTemporaryView("sensorReading", table);

        // 执行sql查询
        // 不通过as指定列名
        // Table resTable = tableEnv.sqlQuery("select name, temperature, `timestamp` from sensorReading");
        // 通过as指定列名
        Table resTable = tableEnv.sqlQuery("select name, avg(temp) as avg_temp from sensorReading group by name");

        // table -> dataStream
        /*
            根据查询的类型，在许多情况下，生成的动态表是一个管道，它不仅在将表覆盖到数据流时产生仅插入更改，
            而且还产生撤回和其他类型的更新。在 table-to-stream 转换期间，这可能会导致类似于

            Table sink 'Unregistered_DataStream_Sink_1' doesn't support consuming update changes [...].

            在这种情况下，需要再次修改查询或切换到 toChangelogStream。
            以下示例显示了如何转换更新表。每个结果行都代表更改日志中的一个条目，带有更改标志，
            可以通过调用 row.getKind() 对其进行查询。在示例中，Alice 的第二个分数在 (-U) 更改之前和 (+U) 更改之后创建更新。
         */
        // 仅插入更改
        // DataStream<Row> outputDataStream = tableEnv.toDataStream(resTable);
        // 改变
        DataStream<Row> outputDataStream = tableEnv.toChangelogStream(resTable);

        // 打印
        // +I[sensor-1, 2000, 20.6] 插入之后， -U是更新之前，+U是更新之后
        outputDataStream.printToErr();

        env.execute("D03_CreateStreamTableEnvironment");
    }
}
