package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

/**
 * 需求：监控温度传感器的温度值，如果温度值在10秒钟之内 (processing time)连续上升， 则报警。
 *
 * @author lanyangji
 * @date 2021/7/7 9:46
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D13_TimeService
 */
public class D13_TimeService {
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
                .process(new TempWarningKeyedProcessFunction(10))
                .printToErr();

        env.execute("D13_TimeService");
    }

    // 温度警告处理函数
    public static final class TempWarningKeyedProcessFunction extends KeyedProcessFunction<String, SensorReading, String> {
        private final Integer interval;

        // 上一次温度状态
        private ValueState<Double> lastTempState;
        // 当前定时器时间戳
        private ValueState<Long> timerTsState;

        public TempWarningKeyedProcessFunction(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.lastTempState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last_temp_state", Double.class)
            );

            this.timerTsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("timer_ts_state", Long.class)
            );
        }

        @Override
        public void processElement(SensorReading sensorReading, Context ctx, Collector<String> out) throws Exception {
            // 取出状态
            Double lastTemp = this.lastTempState.value();
            if (null == lastTemp) {
                lastTemp = Double.MIN_VALUE;
                this.lastTempState.update(lastTemp);
            }
            Long timerTs = this.timerTsState.value();

            // 当前温度
            Double currentTemp = sensorReading.getTemperature();
            // 如果温度相较于上次上升了，并且没有设置定时器时间戳
            if (currentTemp > lastTemp && null == timerTs) {
                // 注册定时器
                long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                // 更新状态中保存的定时器时间戳
                this.timerTsState.update(ts);
            } else if (currentTemp < lastTemp && null != timerTs) {
                // 如果温度下降了，且定时器时间戳状态中有值，要移除定时器
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                this.timerTsState.clear();
            }

            // 更新上次温度状态值
            this.lastTempState.update(currentTemp);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(String.format("%s 温度在 %d 秒内连续上升", ctx.getCurrentKey(), interval));
            // 清空timer状态
            this.timerTsState.clear();
        }
    }
}
