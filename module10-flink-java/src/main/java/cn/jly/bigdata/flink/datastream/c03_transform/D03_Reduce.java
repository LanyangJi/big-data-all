package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author jilanyang
 * @date 2021/7/1 16:24
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D03_Reduce
 */
public class D03_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从内存集合中读取
        DataStreamSource<SensorReading> sensorDataStream = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor_1", 1547718199L, 35.8),
                        new SensorReading("sensor_6", 1547718201L, 15.4),
                        new SensorReading("sensor_7", 1547718202L, 6.7),
                        new SensorReading("sensor_10", 1547718205L, 38.1),
                        new SensorReading("sensor_6", 1547718207L, 38.1),
                        new SensorReading("sensor_6", 1547718209L, 11.4)
                )
        );

        // 分组
        KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = sensorDataStream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) throws Exception {
                return value.getName();
            }
        });

        // reduce聚合 -> 求最小的温度值，并更新当前的时间戳
        SingleOutputStreamOperator<SensorReading> singleOutputStreamOperator = sensorReadingStringKeyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getName(), value2.getTimestamp(), Math.min(value1.getTemperature(), value2.getTemperature()));
            }
        });

        // print
        singleOutputStreamOperator.print();

        env.execute("D03_Reduce");
    }
}
