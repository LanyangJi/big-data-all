package cn.jly.bigdata.flink_advanced.table;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.plan.nodes.calcite.Rank;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Over window 聚合是从标准 SQL（OVER 子句）中得知的，并在查询的 SELECT 子句中定义。
 * 与在 GROUP BY 子句中指定的组窗口不同，窗口上方不会折叠行。相反，在窗口聚合上为每个输入行在其相邻行的范围内计算聚合。
 * <p>
 * OverWindow 定义了计算聚合的行范围。 OverWindow 不是用户可以实现的接口。相反，Table API 提供了 Over 类来配置覆盖窗口的属性。
 * 可以在事件时间或处理时间以及指定为时间间隔或行计数的范围内定义窗口。支持的窗口定义作为 Over（和其他类）上的方法公开，并在下面列出：
 * <p>
 * partition by(可选):
 * 在一个或多个属性上定义输入的分区。每个分区都单独排序，聚合函数分别应用于每个分区。
 * ！！！！！！！！！！！！ 注意：在流环境中，如果窗口包含 partition by 子句，则只能并行计算窗口聚合。如果没有 partitionBy(…)，流由单个非并行任务处理。
 * <p>
 * order by(必填)：
 * 定义每个分区内行的顺序，从而定义聚合函数应用于行的顺序。
 * ！！！！！！！！！！ 注意：对于流式查询，这必须是声明的事件时间或处理时间时间属性。目前，仅支持单个排序属性。
 * <p>
 * preceding(可选):
 * 定义包含在窗口中并位于当前行之前的行的间隔。间隔可以指定为时间或行计数间隔。
 * 有界窗口用间隔的大小指定，例如，10.minutes 表示时间间隔或 10.rows 表示行计数间隔。
 * 使用常量指定无界的窗口，即时间间隔的 UNBOUNDED_RANGE 或行计数间隔的 UNBOUNDED_ROW。 Unbounded over windows 从分区的第一行开始。
 * 如果省略前面的子句，则使用 UNBOUNDED_RANGE 和 CURRENT_RANGE 作为窗口的默认前后。
 * <p>
 * following(可选):
 * 定义包含在窗口中并跟随当前行的行的窗口间隔。间隔必须以与前一个间隔（时间或行计数）相同的单位指定。
 * 目前，不支持在当前行之后有行的窗口。相反，您可以指定两个常量之一：
 * CURRENT_ROW 将窗口的上限设置为当前行。
 * CURRENT_RANGE 将窗口的上限设置为当前行的排序键，即与当前行具有相同排序键的所有行都包含在窗口中。
 * 如果省略以下子句，则时间间隔窗口的上限定义为 CURRENT_RANGE，行计数间隔窗口的上限定义为 CURRENT_ROW。
 * <p>
 * as(必填)：
 * 为上窗指定别名。别名用于在以下 select() 子句中引用覆盖窗口。
 * ！！！！！！！！！！！！！注意：目前，同一个 select() 调用中的所有聚合函数都必须在同一个窗口上计算。
 * <p>
 * ========================= 无界over窗口 ==================
 * Unbounded Over Windows
 * // Unbounded Event-time over window (assuming an event-time attribute "rowtime")
 * .window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(UNBOUNDED_RANGE).as("w"));
 * <p>
 * // Unbounded Processing-time over window (assuming a processing-time attribute "proctime")
 * .window(Over.partitionBy($("a")).orderBy("proctime").preceding(UNBOUNDED_RANGE).as("w"));
 * <p>
 * // Unbounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
 * .window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(UNBOUNDED_ROW).as("w"));
 * <p>
 * // Unbounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
 * .window(Over.partitionBy($("a")).orderBy($("proctime")).preceding(UNBOUNDED_ROW).as("w"));
 * <p>
 * ========================= 有界over窗口 ==================
 * Bounded Over Windows
 * // Bounded Event-time over window(assuming an event-time attribute"rowtime")
 * .window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(lit(1).minutes()).as("w"));
 * <p>
 * // Bounded Processing-time over window (assuming a processing-time attribute "proctime")
 * .window(Over.partitionBy($("a")).orderBy($("proctime")).preceding(lit(1).minutes()).as("w"));
 * <p>
 * // Bounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
 * .window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(rowInterval(10)).as("w"));
 * <p>
 * // Bounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
 * .window(Over.partitionBy($("a")).orderBy($("proctime")).preceding(rowInterval(10)).as("w"));
 *
 * @author jilanyang
 * @createTime 2021/8/19 11:26
 */
public class D26_TableApi_Over_Window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // source - 订单流
        SingleOutputStreamOperator<Order> orderDS = env.socketTextStream("localhost", 9999)
                .flatMap(
                        new FlatMapFunction<String, Order>() {
                            @Override
                            public void flatMap(String s, Collector<Order> collector) throws Exception {
                                String[] fields = s.split(",");
                                String orderId = fields[0];
                                String userId = fields[1];
                                long createTime = Long.parseLong(fields[2]);
                                double money = Double.parseDouble(fields[3]);

                                collector.collect(new Order(orderId, userId, createTime, money));
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        // 分配时间戳和水印，允许3秒的延迟
                        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Order>() {
                                            @Override
                                            public long extractTimestamp(Order order, long l) {
                                                return order.getCreateTime();
                                            }
                                        }
                                )
                );

        // dataStream -> table
        Table orderTable = tableEnv.fromDataStream(
                orderDS,
                $("orderId"),
                $("userId"),
                $("createTime").rowtime(),
                $("money")
        );

        // over window
        Table overTable = orderTable.window(
                Over.partitionBy($("userId"))
                        .orderBy($("createTime").desc())
                        .preceding(lit(1).second())
                        .as("w")
        )
                .select(
                        $("userId"),
                        $("createTime")
                );

        // 输出
        tableEnv.toRetractStream(overTable, Row.class).print();

        env.execute("D26_TableApi_Over_Window");
    }
}
