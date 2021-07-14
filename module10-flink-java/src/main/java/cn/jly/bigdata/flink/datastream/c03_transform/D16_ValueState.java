package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author lanyangji
 * @date 2021/7/7 17:23
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D16_ValueState
 */
public class D16_ValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("linux01", 9999)
                .map(new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String value) throws Exception {
                        return JSON.parseObject(value, SensorReading.class);
                    }
                })
                .keyBy(SensorReading::getName)
                .process(new HugeTempDiffKeyedProcessFunction(10))
                .printToErr();

        env.execute("D16_ValueState");
    }

    public static final class HugeTempDiffKeyedProcessFunction extends KeyedProcessFunction<String, SensorReading, String> {
        // 最大温度差
        private final Integer maxTempDiff;

        // 存储上一次温度的状态
        private ValueState<Double> lastTempState;

        public HugeTempDiffKeyedProcessFunction(Integer maxTempDiff) {
            this.maxTempDiff = maxTempDiff;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last_temp_state", Double.class));
        }

        @Override
        public void processElement(SensorReading sensorReading, Context ctx, Collector<String> out) throws Exception {
            // 取出状态，获取上一次温度
            Double lastTemp = this.lastTempState.value();
            if (lastTemp == null) {
                this.lastTempState.update(sensorReading.getTemperature());
                return;
            }

            // 比较本次与上次的温差
            if (Math.abs(sensorReading.getTemperature() - lastTemp) > this.maxTempDiff) {
                this.lastTempState.update(sensorReading.getTemperature());
                out.collect(String.format("%s 最近两次的温差查过 %d，分别为 %f, %f", sensorReading.getName(), this.maxTempDiff,
                        lastTemp, sensorReading.getTemperature()));
            }
        }
    }

}
