package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import cn.jly.bigdata.flink.datastream.c02_source.D06_CustomRandomSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 计数窗口
 * CountWindow根据窗口中相同 key 元素的数量来触发执行，执行时只计算元素数量达到窗口大小的 key 对应的结果。
 * 注意：
 * CountWindow 的 window _size 指的是相同 Key 的元素的个数，不是输入的所有元素的总数。
 *
 * @author lanyangji
 * @date 2021/7/6 14:16
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D09_CountWindow
 */
public class D09_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<SensorReading, String> sensorReadingKeyedStream = env.addSource(new D06_CustomRandomSource.RandomSource(Arrays.asList(
                "sensor-02",
                "sensor-03",
                "sensor-04",
                "sensor-05",
                "sensor-06"
        )))
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading value) throws Exception {
                        return value.getName();
                    }
                });

        // 滚动窗口
        sensorReadingKeyedStream.countWindow(5)
                .maxBy("temperature")
                .print();

        // 滑动窗口
        sensorReadingKeyedStream.countWindow(15, 5)
                .maxBy("temperature")
                .printToErr();

        env.execute("D09_CountWindow");
    }
}
