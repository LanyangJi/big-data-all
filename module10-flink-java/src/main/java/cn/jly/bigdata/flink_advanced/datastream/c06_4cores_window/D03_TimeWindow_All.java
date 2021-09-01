package cn.jly.bigdata.flink_advanced.datastream.c06_4cores_window;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;

/**
 * 基于窗口的完成数据操作，涉及分配时间戳和水印、allowedLateness、侧输出流输出迟到数据
 * kafka为数据源
 * <p>
 * 统计过去5秒钟，用户的订单总额
 *
 * @author jilanyang
 * @date 2021/8/31 16:40
 */
public class D03_TimeWindow_All {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux01:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "lanyangji");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // kafkaSource
        FlinkKafkaConsumer<String> kafkaSourceFunction = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSourceFunction);

        // 分配水印和时间戳
        SingleOutputStreamOperator<Order> orderDSWithTs = kafkaDS.map(
                new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        return JSON.parseObject(value, Order.class);
                    }
                }
        )
                .assignTimestampsAndWatermarks(
                        // 允许3秒的延迟和乱序
                        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Order>() {
                                            @Override
                                            public long extractTimestamp(Order element, long recordTimestamp) {
                                                return element.getCreateTime();
                                            }
                                        }
                                )
                );

        // 迟到数据侧输出流标签
        final OutputTag<Order> lateDataTag = new OutputTag<Order>("late_data", TypeInformation.of(Order.class)) {
        };

        // 开窗聚合
        SingleOutputStreamOperator<String> aggWindowDS = orderDSWithTs.keyBy(Order::getUserId)
                .window(
                        // 5秒的滚动窗口
                        TumblingEventTimeWindows.of(Time.seconds(5))
                )
                // 再水印允许的延迟基础上，再允许迟到2秒
                // 说明，当水印到达并等于窗口的结束时间时，会触发窗口发生聚合计算并得到结果；此时如果在加下来的2秒内还有数据到达，则每到达一次，
                //      便触发计算一次，更新窗口的结果
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(lateDataTag)
                .aggregate(
                        // 增量聚合函数
                        new OrderTotalMoneyAggFunc(),
                        new OrderTotalMoneyWindowFunc()
                );

        // 常规输出
        aggWindowDS.print("agg");

        // 获取测输出流中的迟到数据
        DataStream<Order> lateDS = aggWindowDS.getSideOutput(lateDataTag);
        lateDS.print("late");

        env.execute("D03_TimeWindow_All");
    }

    // 增量聚合函数
    private static class OrderTotalMoneyAggFunc implements AggregateFunction<Order, Double, Double> {

        @Override
        public Double createAccumulator() {
            return 0d;
        }

        @Override
        public Double add(Order value, Double accumulator) {
            return value.getMoney() + accumulator;
        }

        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }

    // 全窗口函数
    private static class OrderTotalMoneyWindowFunc extends ProcessWindowFunction<Double, String, String, TimeWindow> {
        private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void process(String key, Context context, Iterable<Double> elements, Collector<String> out) throws Exception {
            Double totalMoney = elements.iterator().next();

            StringBuilder sb = new StringBuilder();
            sb.append(key).append("在").append(format.format(new Date(context.window().getStart())))
                    .append("到").append(format.format(new Date(context.window().getEnd())))
                    .append("的销售总额为").append(totalMoney);

            out.collect(sb.toString());
        }
    }
}
