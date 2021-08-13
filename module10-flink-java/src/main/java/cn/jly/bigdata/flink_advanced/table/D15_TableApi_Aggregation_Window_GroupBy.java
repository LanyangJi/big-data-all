package cn.jly.bigdata.flink_advanced.table;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 在组窗口上对表进行分组和聚合，可能还有一个或多个分组键。
 *
 * @author jilanyang
 * @createTime 2021/8/13 14:32
 */
public class D15_TableApi_Aggregation_Window_GroupBy {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1); // 为了方便查看，这边并行度设置为1
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // source
        SingleOutputStreamOperator<Order> orderDS = env.socketTextStream("linux01", 9999)
                .flatMap(new FlatMapFunction<String, Order>() {
                    @Override
                    public void flatMap(String s, Collector<Order> collector) throws Exception {
                        String[] fields = s.split(",");
                        String orderId = fields[0];
                        String userId = fields[1];
                        long createTime = Long.parseLong(fields[2]);
                        double money = Double.parseDouble(fields[3]);

                        collector.collect(new Order(orderId, userId, createTime, money));
                    }
                })
                // 指定水印生成规则
                .assignTimestampsAndWatermarks(
                        // 允许3秒的延迟
                        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                            @Override
                            public long extractTimestamp(Order order, long l) {
                                return order.getCreateTime();
                            }
                        })
                );

        // DataStream -> Table
        Table orderTable = tableEnv.fromDataStream(
                orderDS,
                $("orderId"),
                $("userId"),
                $("createTime").rowtime(), // 必须在table中显式指定某一列为rowTime，否则会报错：A group window expects a time attribute for grouping in a stream environment.
                $("money")
        );

        // window, group by
        Table windowAggTable = orderTable.window(
                // 5秒的滚动窗口
                Tumble.over(lit(5).second()).on($("createTime")).as("w")
        )
                .groupBy($("w"), $("userId"))
                .select(
                        $("userId"),
                        $("w").start().as("w_start"),
                        $("w").end().as("w_end"),
                        $("money").max().as("max_money")
                );

        // 输出
        DataStream<Tuple2<Boolean, Row>> resDS = tableEnv.toRetractStream(windowAggTable, Row.class);
        resDS.print();

        env.execute("D15_TableApi_Aggregation_Window_GroupBy");
    }
}
