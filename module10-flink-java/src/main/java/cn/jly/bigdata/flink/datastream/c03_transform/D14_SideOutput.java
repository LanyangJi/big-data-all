package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 监控传感器温度，将低于30的通过侧输出流输出
 *
 * @author lanyangji
 * @date 2021/7/7 10:53
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D14_SideOutput
 */
public class D14_SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 低温侧输出流outputTag
        OutputTag<SensorReading> lowTempSensorOutputTag = new OutputTag<SensorReading>("low_temp_sensor") {
        };

        SingleOutputStreamOperator<SensorReading> processDataStream = env.socketTextStream("linux01", 9999)
                .map(new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String value) throws Exception {
                        return JSON.parseObject(value, SensorReading.class);
                    }
                })
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                        if (value.getTemperature() < 30d) {
                            // 侧输出
                            ctx.output(lowTempSensorOutputTag, value);
                        } else {
                            out.collect(value);
                        }
                    }
                });

        // 获取侧输出流
        DataStream<SensorReading> lowTempDataStream = processDataStream.getSideOutput(lowTempSensorOutputTag);

        // 输出
        processDataStream.print("high");
        lowTempDataStream.printToErr("low");

        env.execute("D14_SideOutput");
    }
}
