package cn.jly.bigdata.flink_advanced.datastream.c08_4cores_state;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.metrics.Sensor;

/**
 * 无状态计算：
 * 不需要考虑历史数据，换句话说就是相同的输入得到相同的输出，比如map/flatMap/filter...
 * <p>
 * flink中的状态分为Managed State & Raw State
 * 1. ManagerState--开发中推荐使用 : Fink自动管理/优化,支持多种数据结构
 * ① KeyState--只能在keyedStream上使用,支持多种数据结构
 * ② OperatorState--一般用在Source上,支持ListState
 * 2. RawState--完全由用户自己管理,只支持byte[],只能在自定义Operator上使用
 * ① OperatorState
 * <p>
 * 本例需求：
 * 同一个传感器连续两次的温差超过10度报警
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c07_4cores_state
 * @class D01_State
 * @date 2021/8/1 11:38
 */
public class D01_State_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        SingleOutputStreamOperator<SensorReading> sensorReadingDs = env.socketTextStream("linux01", 9999)
                .map(new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                    }
                });

        // 分组
        KeyedStream<SensorReading, String> keyedStream = sensorReadingDs.keyBy(SensorReading::getName);

        // 计算
        keyedStream.process(new TempDiffKeyedProcessFunction(10d))
                .print();

        env.execute("D01_State_KeyedState");
    }

    public static class TempDiffKeyedProcessFunction extends KeyedProcessFunction<String, SensorReading, String> {
        // 值状态，存储上一次温度
        private ValueState<Double> lastTempState = null;

        // 最大温差上限制
        private final Double maxTempDiff;

        public TempDiffKeyedProcessFunction(Double maxTempDiff) {
            this.maxTempDiff = maxTempDiff;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 状态描述器
            ValueStateDescriptor<Double> stateDescriptor = new ValueStateDescriptor<>("last_temp", Double.class);
            lastTempState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(SensorReading sensorReading, KeyedProcessFunction<String, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = lastTempState.value();
            if (lastTemp == null) {
                lastTempState.update(sensorReading.getTemperature());
                return;
            }

            Double tempDifferent = Math.abs(sensorReading.getTemperature() - lastTemp);
            if (tempDifferent > this.maxTempDiff) {
                out.collect(String.format("%s 连续两次的温差超过 %f, 真实温差为 %f", sensorReading.getName(), maxTempDiff, tempDifferent));
            }
            lastTempState.update(sensorReading.getTemperature());
        }

        public Double getMaxTempDiff() {
            return maxTempDiff;
        }
    }
}
