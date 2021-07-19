package cn.jly.bigdata.flink.table;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * DSL风格
 *
 * @author jilanyang
 * @date 2021年7月14日22:21:51
 */
public class D04_TableApi {
    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取数据
        SingleOutputStreamOperator<SensorReading> sensorReadingDS = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String s) throws Exception {
                        return JSON.parseObject(s, SensorReading.class);
                    }
                });

        // 基于数据流创建table
        Table sensorReadingTable = tableEnv.fromDataStream(sensorReadingDS).as("name", "ts", "temp");

        // 示例1：table api方式 -> 过滤出大于40度的传感器数据
        Table resTable = sensorReadingTable.select($("name"), $("ts"), $("temp"))
                .where($("temp").isGreater(40d));

        // 转换为outputDS并打印输出
        DataStream<Row> outputDS = tableEnv.toChangelogStream(resTable);
        outputDS.print("示例1：Table Api方式");

        // 示例2：sql方式 -> 过滤出大于40度的传感器数据
        // 创建视图名
        tableEnv.createTemporaryView("tbl_sensor", sensorReadingDS);
        // 执行sql
        String sql = "select `name`, `temperature`, `timestamp` from tbl_sensor where `temperature` > 40";
        Table sqlResTable = tableEnv.sqlQuery(sql);

        // 转换为DataStream输出
        tableEnv.toChangelogStream(sqlResTable)
                .printToErr("示例2：sql方式");

        env.execute("D04_TableApi");
    }
}
