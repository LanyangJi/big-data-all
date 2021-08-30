package cn.jly.bigdata.project.statistical_analysis;

import cn.hutool.core.collection.CollUtil;
import cn.jly.bigdata.project.beans.ItemViewCount;
import cn.jly.bigdata.project.beans.UserBehavior;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

/**
 * 每5秒钟统计过去10秒钟的热门商品排行
 *
 * @author jilanyang
 * @date 2021/8/29 21:26
 */
public class D02_HotItems_Kafka {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        // 1. 读取数据 - 带时间戳和水印的用户行为数据流
        // DataStreamSource<String> userBehaviorDS = env.readTextFile("D:\\JLY\\test\\Data\\UserBehavior.csv");

        // 2. kafka数据源
        Properties properties = new Properties();
        // Kafka集群地址
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux01:9092");
        // 消费者id
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 自动重置偏移量，记录了偏移量就从偏移量位置读，否则重置为配置的最新
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 允许自动提交
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // kv反序列化
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        FlinkKafkaConsumer<String> kafkaSourceFunction = new FlinkKafkaConsumer<>(
                "test",
                new SimpleStringSchema(),
                properties
        );
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSourceFunction);

        kafkaDS.map(
                        new MapFunction<String, UserBehavior>() {
                            @Override
                            public UserBehavior map(String value) throws Exception {
                                return JSON.parseObject(value, UserBehavior.class);
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        // 给升序数据指定时间戳和水印
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<UserBehavior>() {
                                            @Override
                                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                                return element.getTimestamp();
                                            }
                                        }
                                )
                )
                .filter(
                        new FilterFunction<UserBehavior>() {
                            @Override
                            public boolean filter(UserBehavior value) throws Exception {
                                return "pv".equalsIgnoreCase(value.getBehavior());
                            }
                        }
                )
                .keyBy(UserBehavior::getItemId)
                .window(
                        // 每5秒统计过去10秒的数据
                        SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))
                )
                .aggregate(
                        // 增量聚合函数
                        new AggregateFunction<UserBehavior, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(UserBehavior value, Long accumulator) {
                                return accumulator + 1;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return a + b;
                            }
                        },

                        // 全窗口函数
                        new WindowFunction<Long, ItemViewCount, Long, TimeWindow>() {
                            private final ItemViewCount itemViewCount = new ItemViewCount();

                            @Override
                            public void apply(Long key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
                                long sum = 0L;
                                for (Long acc : input) {
                                    sum += acc;
                                }

                                this.itemViewCount.setCount(sum);
                                this.itemViewCount.setItemId(key);
                                this.itemViewCount.setWindowEnd(window.getEnd());

                                out.collect(this.itemViewCount);
                            }
                        }
                )
                .keyBy(
                        // 按照窗口分组
                        new KeySelector<ItemViewCount, Long>() {
                            @Override
                            public Long getKey(ItemViewCount value) throws Exception {
                                return value.getWindowEnd();
                            }
                        }
                )
                .process(
                        new KeyedProcessFunction<Long, ItemViewCount, String>() {
                            // 定义状态，存储每个窗口对应的所有itemViewCount
                            private ListState<ItemViewCount> listState = null;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 状态的初始化
                                this.listState = getRuntimeContext().getListState(
                                        new ListStateDescriptor<ItemViewCount>("ItemViewCount", ItemViewCount.class)
                                );
                            }

                            @Override
                            public void processElement(ItemViewCount value, KeyedProcessFunction<Long, ItemViewCount, String>.Context ctx, Collector<String> out) throws Exception {
                                this.listState.add(value);

                                // 定义一个100ms之后的定时器，用以对状态中的数据进行排序并输出topN
                                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 100L);
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<Long, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                // 从状态中取出当前窗口对应的所有itemViewCount
                                Iterable<ItemViewCount> iterable = this.listState.get();
                                if (CollUtil.isNotEmpty(iterable)) {
                                    ArrayList<ItemViewCount> list = Lists.newArrayList(iterable);
                                    list.sort(
                                            new Comparator<ItemViewCount>() {
                                                @Override
                                                public int compare(ItemViewCount o1, ItemViewCount o2) {
                                                    return -o1.getCount().compareTo(o2.getCount());
                                                }
                                            }
                                    );

                                    StringBuilder sb = new StringBuilder();
                                    sb.append("=============================").append("\n")
                                            .append("窗口的结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

                                    // 打印出top10
                                    for (int i = 0; i < Math.min(10, list.size()); i++) {
                                        ItemViewCount currentItemViewCount = list.get(i);
                                        sb.append("No").append(i + 1).append("，")
                                                .append(" 商品id:").append(currentItemViewCount.getItemId())
                                                .append(" ，热门度：").append(currentItemViewCount.getCount()).append("\n");
                                    }

                                    sb.append("=============================").append("\n");

                                    out.collect(sb.toString());
                                }
                            }
                        }
                )
                .print();


        env.execute("D02_HotItems_Kafka");
    }
}
