package cn.jly.bigdata.project.statistical_analysis;

import cn.jly.bigdata.project.beans.ItemViewCount;
import cn.jly.bigdata.project.beans.UserBehavior;
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
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

/**
 * 电商用户行为分析案例
 * <p>
 * 1. 实时统计分析
 * - 实时热门商品统计
 * - 实时热门页面流量统计
 * - 实时访问流量统计
 * - app市场推广统计
 * - 页面广告点击量统计
 * <p>
 * 2. 业务流程及风险控制
 * - 页面广告黑名单过滤
 * - 恶意登录监控
 * - 订单支付实现监控
 * - 支付实时对账
 * <p>
 * 本案例：
 * 每5分钟统计过去1小时的热门商品排行
 *
 * @author jilanyang
 * @date 2021/8/29 11:50
 */
public class D01_HotItems {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        // 1. 读取数据 - 带时间戳和水印的用户行为数据流
        DataStreamSource<String> lineDS = env.readTextFile("D:\\JLY\\test\\Data\\UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = lineDS.map(
                        // 这里new MapFunction<..>()实际上是将一个MapFunction类型的对象传递给map方法，妥妥一个java对象，需要被序列化传播到各个分区中，
                        // 继而进行并行计算，所以要求MapFunction可以被序列化，即要求其所有成员变量可以序列化
                        new MapFunction<String, UserBehavior>() {
                            // 作为MapFunction的成员变量，要求对象必须可序列化，如果放在map方法中，则不需要
                            // 这里之所以不放在map方法中，是为了进行对象复用，不然每条数据调用一次map方法，就会创建一个UserBehavior对象，
                            //      增加JVM进行垃圾回收的开销
                            private final UserBehavior userBehavior = new UserBehavior();

                            @Override
                            public UserBehavior map(String value) throws Exception {
                                String[] fields = value.split(",");


                                userBehavior.setUserId(Long.parseLong(fields[0]));
                                userBehavior.setItemId(Long.parseLong(fields[1]));
                                userBehavior.setCategoryId(Integer.parseInt(fields[2]));
                                userBehavior.setBehavior(fields[3]);
                                userBehavior.setTimestamp(Long.parseLong(fields[4]));

                                return userBehavior;
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        // 分配时间戳和水印：允许3秒的延迟和乱序
//                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                                .withTimestampAssigner(
//                                        new SerializableTimestampAssigner<UserBehavior>() {
//                                            @Override
//                                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        // 数据源以秒为单位
//                                                return element.getTimestamp() * 1000L;
//                                            }
//                                        }
//                                )
                        // 由于数据源是单调递增的，这边用单调的时间戳
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<UserBehavior>() {
                                            @Override
                                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                                // 数据源以秒为单位
                                                return element.getTimestamp() * 1000L;
                                            }
                                        }
                                )
                );

        // 分组开窗聚合
        SingleOutputStreamOperator<ItemViewCount> windowAggDS = userBehaviorDS.filter(
                        // 过滤PV型用户行为
                        new FilterFunction<UserBehavior>() {
                            @Override
                            public boolean filter(UserBehavior userBehavior) throws Exception {
                                return "pv".equalsIgnoreCase(userBehavior.getBehavior());
                            }
                        }
                )
                .keyBy(UserBehavior::getItemId)
                .window(
                        // 每5分钟统计过去1小时的用户行为
                        SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5))
                )
                .aggregate(
                        // 增量聚合函数
                        new ItemCountAgg(),
                        // 全窗口函数
                        new WindowItemCountResult()
                );

        // 收集同一窗口的所有商品count数据，排序输出topN
        // 按照窗口结束时间分组，这样同一个窗口的所有数据会被收集到一起
        SingleOutputStreamOperator<String> processDS = windowAggDS.keyBy(ItemViewCount::getWindowEnd)
                .process(
                        // 用自定义KeyedProcessFunction去计算同一窗口的热门商品topN
                        // 这边是排序取前五
                        new TopNHotItems(5)
                );

        // 打印输出
        processDS.print("PV TOP 5");

        env.execute("D01_HotItems");

    }

    /**
     * 增量聚合函数
     * <p>
     * 泛型类型：
     * 1. 输入类型
     * 2. 累加器类型
     * 3. 输出类型，这里直接输出累加器到全窗口函数
     */
    private static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    /**
     * 全窗口函数
     * <p>
     * 泛型
     * 1. 增量聚合函数的输出
     * 2. 窗口函数的最终输出类型
     * 3. key类型，这里是itemId
     * 4. 窗口类型
     */
    private static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {
        private final ItemViewCount itemViewCount = new ItemViewCount();

        @Override
        public void apply(Long key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            long sum = 0L;
            for (Long acc : input) {
                sum += acc;
            }

            this.itemViewCount.setItemId(key);
            this.itemViewCount.setCount(sum);
            this.itemViewCount.setWindowEnd(window.getEnd());

            out.collect(this.itemViewCount);
        }
    }

    private static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {
        // 收集top几
        private final int topNum;

        // 状态，存储每个窗口的所有itemViewCount数据
        private ListState<ItemViewCount> itemViewCountListState = null;

        public TopNHotItems(int topNum) {
            this.topNum = topNum;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.itemViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("ItemViewCount", ItemViewCount.class)
            );
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, KeyedProcessFunction<Long, ItemViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            this.itemViewCountListState.add(itemViewCount);

            // 定义一个100ms之后的定时器，用以对状态中的数据进行排序并输出topN
            ctx.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 100L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterable<ItemViewCount> iterable = this.itemViewCountListState.get();
            if (iterable != null) {
                ArrayList<ItemViewCount> arrayList = Lists.newArrayList(iterable);
                // 排序, 倒序排序
                arrayList.sort(
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

                for (int i = 0; i < Math.min(topNum, arrayList.size()); i++) {
                    ItemViewCount currentItemViewCount = arrayList.get(i);

                    sb.append("No").append(i + 1).append("，")
                            .append(" 商品id:").append(currentItemViewCount.getItemId())
                            .append(" ，热门度：").append(currentItemViewCount.getCount()).append("\n");
                }
                sb.append("=============================").append("\n");

                out.collect(sb.toString());

                // 控制输出频率，因为我们是基于文件的，真实情况下不会那么快
                Thread.sleep(1000L);
            }
        }
    }
}
