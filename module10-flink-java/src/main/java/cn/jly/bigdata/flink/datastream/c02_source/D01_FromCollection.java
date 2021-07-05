package cn.jly.bigdata.flink.datastream.c02_source;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author jilanyang
 * @date 2021/6/30 0030 13:12
 * @packageName cn.jly.bigdata.flink.datastream.c02_source
 * @className D01_FromCollection
 */
public class D01_FromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(12);

        // 从内存集合中读取
        DataStreamSource<SensorReading> sensorDataStream = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor_1", 1547718199L, 35.8),
                        new SensorReading("sensor_6", 1547718201L, 15.4),
                        new SensorReading("sensor_7", 1547718202L, 6.7),
                        new SensorReading("sensor_10", 1547718205L, 38.1)
                )
        );

        sensorDataStream.map(new MapFunction<SensorReading, String>() {
            @Override
            public String map(SensorReading sensorReading) throws Exception {
                return JSON.toJSONString(sensorReading);
            }
        })
                .print();

        env.execute("D01_FromCollection");
    }
}
