package cn.jly.bigdata.flink_advanced.datastream.c06_4cores_window;

import cn.jly.bigdata.flink_advanced.datastream.beans.SignalCar;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * 基于次数的滑动窗口
 *
 * @author jilanyang
 * @date 2021/7/29 14:00
 * @package cn.jly.bigdata.flink_advanced.datastream.c06_4cores_window
 * @class D04_CountWindow_Sliding
 */
public class D04_CountWindow_Sliding {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        SingleOutputStreamOperator<SignalCar> signalCarDs = env.socketTextStream("linux01", 9999)
                .map(new MapFunction<String, SignalCar>() {
                    @Override
                    public SignalCar map(String value) throws Exception {
                        return JSON.parseObject(value, SignalCar.class);
                    }
                });

        // 开窗统计
        SingleOutputStreamOperator<SignalCar> countDs = signalCarDs.keyBy(SignalCar::getSignalId)
                .countWindow(10L, 5L)
                .apply(new WindowFunction<SignalCar, SignalCar, String, GlobalWindow>() {
                    @Override
                    public void apply(String key, GlobalWindow window, Iterable<SignalCar> input, Collector<SignalCar> out) throws Exception {
                        long sum = 0;
                        for (SignalCar signalCar : input) {
                            sum += signalCar.getPassingCarCount();
                        }
                        out.collect(new SignalCar(key, sum));
                    }
                });

        // 打印输出
        countDs.printToErr();

        env.execute("D04_CountWindow_Sliding");
    }
}
