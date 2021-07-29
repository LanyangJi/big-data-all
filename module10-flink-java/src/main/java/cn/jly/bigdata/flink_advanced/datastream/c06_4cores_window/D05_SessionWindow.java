package cn.jly.bigdata.flink_advanced.datastream.c06_4cores_window;

import cn.jly.bigdata.flink_advanced.datastream.beans.SignalCar;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 会话窗口_基于处理（processing time）时间
 *
 * <p>
 * 需求：
 * 如果连续10s没有接到数据，则触发当前窗口的计算
 * <p>
 * 触发规则：
 * 超过指定的时间间隔没有新的数据来到（前提是对应的key先前已经来过数据了）
 *
 * @author jilanyang
 * @date 2021/7/29 15:26
 * @package cn.jly.bigdata.flink_advanced.datastream.c06_4cores_window
 * @class D05_SessionWindow
 */
public class D05_SessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.socketTextStream("linux01", 9999)
                .map(new MapFunction<String, SignalCar>() {
                    @Override
                    public SignalCar map(String value) throws Exception {
                        return JSON.parseObject(value, SignalCar.class);
                    }
                })
                .keyBy(SignalCar::getSignalId)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .apply(new WindowFunction<SignalCar, SignalCar, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<SignalCar> input, Collector<SignalCar> out) throws Exception {
                        long sum = 0;
                        for (SignalCar signalCar : input) {
                            sum += signalCar.getPassingCarCount();
                        }
                        out.collect(new SignalCar(key, sum));
                    }
                })
                .printToErr();

        env.execute("D05_SessionWindow");
    }
}
