package cn.jly.bigdata.flink_advanced.datastream.c06_4cores_window;

import cn.jly.bigdata.flink_advanced.datastream.beans.SignalCar;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 基于时间的滚动窗口示例 _ 基于处理时间（processing time）
 * <p>
 * 需求：
 * 每5秒钟统计一次，最近5秒钟内，经过各个路口的骑车数量
 *
 * @author jilanyang
 * @date 2021/7/29 14:00
 * @package cn.jly.bigdata.flink_advanced.datastream.c06_4cores_window
 * @class D01_TimeWindow_Tumbling
 */
public class D01_TimeWindow_Tumbling {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        SingleOutputStreamOperator<SignalCar> signalCarDs = env.socketTextStream("linux01", 9999)
                .map(value -> JSON.parseObject(value, SignalCar.class))
                .returns(Types.POJO(SignalCar.class));

        // 开窗处理
        SingleOutputStreamOperator<SignalCar> windowDs = signalCarDs.keyBy(SignalCar::getSignalId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<SignalCar, SignalCar, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<SignalCar> signalCars, Collector<SignalCar> out) throws Exception {
                        long sum = 0L;
                        for (SignalCar signalCar : signalCars) {
                            sum += signalCar.getPassingCarCount();
                        }
                        out.collect(new SignalCar(key, sum));
                    }
                });

        // 打印输出
        // 这边基于处理时间的窗口计算触发要求：1. 时间到了；2. 有数据
        windowDs.printToErr();

        env.execute("D01_TimeWindow_Tumbling");
    }
}
