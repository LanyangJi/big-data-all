package cn.jly.bigdata.flink.datastream.c02_source;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.RandomUtil;
import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @author lanyangji
 * @date 2021/7/6 10:49
 * @packageName cn.jly.bigdata.flink.datastream.c02_source
 * @className D06_CustomRandomSource
 */
public class D06_CustomRandomSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new RandomSource(Arrays.asList("sensor-01", "sensor-02", "sensor-03")))
                .print();

        env.execute("D06_CustomRandomSource");
    }

    public static class RandomSource extends RichSourceFunction<SensorReading> {

        /**
         * 传感器的id列表
         */
        private List<String> sensorIdList;

        private boolean isStart = true;

        public RandomSource(List<String> sensorIdList) {
            this.sensorIdList = sensorIdList;
        }

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (isStart) {
                if (CollUtil.isNotEmpty(sensorIdList)) {
                    for (String sensorId : sensorIdList) {
                        ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), 40 + RandomUtil.randomDouble(-5, 10)));
                    }
                }

                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.isStart = false;
        }
    }
}
