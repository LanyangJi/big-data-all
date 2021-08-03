package cn.jly.bigdata.flink_advanced.datastream.c07_4cores_time_watermark;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.SerializedOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * 自定义watermark生成方式
 * 这边也验证了watermark理论和计算方式
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c07_4cores_time_watermark
 * @class D06_Window_EventTime_Watermark
 * @date 2021/7/29 21:30
 */
public class D06_Window_EventTime_Watermark_Test {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
        int port = tool.getInt("port", 9999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 为了测试，设置并行度为1，
        // 数据被flink线程处理，在多并行度的情况下，watermark对其会取所有线程中最小的watermark来作为当前水印
        env.setParallelism(1);

        // order source
        SingleOutputStreamOperator<Order> orderDs = env.socketTextStream(host, port)
                .map(new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Order(
                                fields[0],
                                fields[1],
                                Long.parseLong(fields[2].trim()),
                                Double.parseDouble(fields[3].trim())
                        );
                    }
                });

        // 开窗
        SingleOutputStreamOperator<Tuple2<String, Double>> windowDs =
                orderDs.assignTimestampsAndWatermarks(
                                // 允许3秒的数据延迟和乱序，这边通过自定义的方式复现forBoundedOutOfOrderness方式
                                WatermarkStrategy.forGenerator(new WatermarkStrategy<Order>() {
                                            @Override
                                            public WatermarkGenerator<Order> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                                return new WatermarkGenerator<Order>() {
                                                    private String userId;
                                                    private String orderId;
                                                    // 最大延迟或乱序时间为3秒
                                                    private long outOfOrdernessMillis = 3000;
                                                    // 最大时间戳初始值，这么设计是为了初始生成的水印不能小于long的最小值
                                                    private long maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;

                                                    /**
                                                     * 每次来一条消息就会触发计算
                                                     * @param order
                                                     * @param eventTimestamp
                                                     * @param output
                                                     */
                                                    @Override
                                                    public void onEvent(Order order, long eventTimestamp, WatermarkOutput output) {
                                                        this.userId = order.getUserId();
                                                        this.orderId = order.getOrderId();
                                                        System.out.printf("当前收到的数据 userId: %s, orderId: %s \n", userId, orderId);

                                                        // 更新最大时间戳
                                                        this.maxTimestamp = Math.max(this.maxTimestamp, eventTimestamp);
                                                    }

                                                    /**
                                                     * 周期生成水印的方法
                                                     *
                                                     * @param output
                                                     */
                                                    @Override
                                                    public void onPeriodicEmit(WatermarkOutput output) {
                                                        output.emitWatermark(new Watermark(this.maxTimestamp - outOfOrdernessMillis - 1));
                                                    }
                                                };
                                            }
                                        })
                                        .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                                            @Override
                                            public long extractTimestamp(Order order, long recordTimestamp) {
                                                return order.getCreateTime();
                                            }
                                        })

                        )
                        .keyBy(Order::getUserId)
                        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                        .apply(new WindowFunction<Order, Tuple2<String, Double>, String, TimeWindow>() {
                            @Override
                            public void apply(String key, TimeWindow timeWindow, Iterable<Order> iterable, Collector<Tuple2<String, Double>> collector) throws Exception {
                                double sum = 0;
                                for (Order order : iterable) {
                                    sum += order.getMoney();
                                }

                                long start = timeWindow.getStart();
                                long end = timeWindow.getEnd();
                                System.out.printf("当前窗口为 [%d, %d)", start, end);

                                collector.collect(Tuple2.of(key, sum));
                            }
                        });

        // 输出统计结果
        windowDs.printToErr();

        env.execute("D06_Window_EventTime_Watermark");
    }
}
