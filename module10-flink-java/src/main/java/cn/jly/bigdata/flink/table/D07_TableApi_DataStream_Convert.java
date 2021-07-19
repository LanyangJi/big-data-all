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
 * dataStream api与table api互转
 * <p>
 * 在DataStream和Table API之间切换会增加一些转换开销。
 * 例如，部分处理二进制数据的表运行时的内部数据结构(如RowData)需要转换为更用户友好的数据结构(如Row)。
 * 通常，这个开销可以忽略，但是这里为了完整性而提到它。
 *
 * 下面的代码展示了如何在两个api之间来回切换的示例。表的列名和类型自动从DataStream的TypeInformation派生出来。
 * 由于DataStream API本身不支持变更日志处理，因此代码在流到表和表到流转换期间假定仅追加/仅插入语义。
 *
 * @author jilanyang
 * @date 2021/7/19 17:13
 */
public class D07_TableApi_DataStream_Convert {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
        int port = tool.getInt("port", 9999);

        // 1. 创建流处理表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. source
//        SingleOutputStreamOperator<SensorReading> sensorReadingDs = env.socketTextStream(host, port)
//                .map(new MapFunction<String, SensorReading>() {
//                    @Override
//                    public SensorReading map(String s) throws Exception {
//                        return JSON.parseObject(s, SensorReading.class);
//                    }
//                });
        DataStreamSource<SensorReading> sensorReadingDs = env.fromElements(
                new SensorReading("sensor-1", 1000L, 35.5d),
                new SensorReading("sensor-2", 1000L, 34.4d),
                new SensorReading("sensor-1", 2000L, 37.2d),
                new SensorReading("sensor-1", 3000L, 38.1d)
        );

        // 3. 创建Table对象
        Table inputTable = tableEnv.fromDataStream(sensorReadingDs).as("name", "ts", "temp");

        // 4. 创建临时视图
        tableEnv.createTemporaryView("tbl_sensor", inputTable);

        // 5. 执行查询
        Table queryTable = tableEnv.sqlQuery("select name, avg(temp) as avg_temp from tbl_sensor group by name");

        // 6. table -> dataStream
        /*
        每个结果行代表一个变更日志中的条目，该条目带有一个变更标志，可以通过对其调用row.getKind()进行查询。
        在这个例子中，Alice的第二个分数在(-U)更改之前和(+U)更改之后创建更新。
         */
        DataStream<Row> queryDs = tableEnv.toChangelogStream(queryTable);

        // 7. 打印
        queryDs.printToErr();

        env.execute("D07_TableApi_DataStream_Convert");
    }
}
