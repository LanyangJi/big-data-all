package cn.jly.bigdata.flink_advanced.test.exer01;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 掌握使用Flink实现模拟双十一实时大屏统计
 * 在大数据的实时处理中，实时的大屏展示已经成了一个很重要的展示项，比如最有名的双十一大屏实时销售总价展示。
 * 除了这个，还有一些其他场景的应用，比如我们在我们的后台系统实时的展示我们网站当前的pv、uv等等今天我们就做一个最简单的模拟电商统计大屏的小例子，
 * 需求如下：
 * 1.实时计算出当天零点截止到当前时间的销售总额 11月11日 00:00:00 ~ 23:59:59
 * 2.计算出各个分类的销售top3
 * 3.每秒钟更新一次统计结果
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.test.exer01
 * @class D01_Real_Big_Screen
 * @date 2021/8/4 21:06
 */
public class D01_Real_Big_Screen_Main_App {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        // source
        DataStreamSource<Tuple2<String, Double>> orderDS = env.addSource(new MySourceFunction());

        // transformation
        SingleOutputStreamOperator<CategoryPojo> tempAggDs = orderDS.keyBy(t -> t.f0)
                // 如果直接使用之前学习的窗口，按照下面的写法表示:
                // 每隔一天计算一次
                // .window(TumblingProcessingTimeWindows.of(Time.days(1))
                // 表示每隔一秒计算过去一天的数据，但是11月11日00:00:01计算的是11月10日00:00:01~11月11日00:00:01 --- 很明显不对
                // .window(SlidingProcessingTimeWindows.of(Time.days(1), Time.seconds(1)))
                /*
                    创建一个新的TumblingProcessingTimeWindows WindowAssigner ，它根据元素时间戳和偏移量将元素分配给时间窗口。
                    例如，如果您希望按小时窗口流，但窗口在每小时的第 15 分钟开始，您可以使用of(Time.hours(1),Time.minutes(15)) ，那么您将获得时间窗口开始在 0:15:00、1:15:00、2:15:00 等
                    相反，如果您生活在不使用 UTC±00:00 时间的地方，例如使用 UTC+08:00 的中国，并且您想要一个大小为一天的时间窗口，并且窗口在每个时间开始当地时间 00:00:00，
                    您可以使用of(Time.days(1),Time.hours(-8)) 。 offset 的参数是Time.hours(-8))因为 UTC+08:00 比 UTC 时间早 8 小时。

                    因此，下面的代码表示从当前的00:00:00开始，计算当前的数据，缺一个触发时机（触发间隔）
                 */
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                // 自定义触发时间间隔（每1秒触发一次）
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                // 自定义复杂的聚合函数，初步聚合，每隔1秒聚合一下各个分类的销售总金额
                .aggregate(
                        new OrderAmountAggregationFunction(),
                        new OrderAmountWindowFunction()
                );

        // 打印输出
        // tempAggDs.print("初步聚合的各个分类的销售额：");

        OutputTag<Tuple2<String, Double>> outputTag = new OutputTag<Tuple2<String, Double>>("total_price") {
        };

        // 使用上面初步聚合的结果(每隔1s聚合一下截止到当前时间销售总金额)
        // 到来的结果可能是乱序的,所以这边按照时间分组,去计算各个时间的销售总额
        SingleOutputStreamOperator<Object> resDs = tempAggDs.keyBy(CategoryPojo::getDateTime)
                // 每隔1s计算一次进行最终的聚合结果输出
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new WindowFinalProcessFunction(outputTag));

        // 正常输出每秒钟的销售额top3
        resDs.printToErr("top3");

        // 获取测输出流，输出每秒钟的总销售额
        resDs.getSideOutput(outputTag)
                .print("total_price");

        env.execute("D01_Real_Big_Screen_Main_App");
    }
}
