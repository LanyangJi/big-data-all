package cn.jly.bigdata.flink_advanced.table;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 类似于 SQL DISTINCT 聚合子句，例如 COUNT(DISTINCT a)。不同的聚合声明聚合函数（内置或用户定义的）仅应用于不同的输入值。
 * Distinct 可以应用于 GroupBy Aggregation、GroupBy Window Aggregation 和 Over Window Aggregation。
 *
 * @author jilanyang
 * @createTime 2021/8/13 14:32
 */
public class D17_TableApi_Aggregation_Distinct {
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
                });

        // DataStream -> table
        Table orderTable = tableEnv.fromDataStream(orderDS);

        // group distinct
        Table resTable = orderTable.groupBy($("userId"))
                .select(
                        $("userId"),
                        // 求和的时候会去重，比如加过3.3了，这个时候又来一个3.3，不会把它放入求和中
                        $("money").sum().distinct().as("sum_distinct_money")
                );

        /*
        // Distinct aggregation on time window group by
            Table groupByWindowDistinctResult = orders
                .window(Tumble
                        .over(lit(5).minutes())
                        .on($("rowtime"))
                        .as("w")
                )
                .groupBy($("a"), $("w"))
                .select($("a"), $("b").sum().distinct().as("d"));

            // Distinct aggregation on over window
            Table result = orders
                .window(Over
                    .partitionBy($("a"))
                    .orderBy($("rowtime"))
                    .preceding(UNBOUNDED_RANGE)
                    .as("w"))
                .select(
                    $("a"), $("b").avg().distinct().over($("w")),
                    $("b").max().over($("w")),
                    $("b").min().over($("w"))
                );

                用户定义的聚合函数也可以与 DISTINCT 修饰符一起使用。要仅计算不同值的聚合结果，只需向聚合函数添加不同的修饰符。
                Table orders = tEnv.from("Orders");

                // Use distinct aggregation for user-defined aggregate functions
                tEnv.registerFunction("myUdagg", new MyUdagg());
                orders.groupBy("users")
                    .select(
                        $("users"),
                        call("myUdagg", $("points")).distinct().as("myDistinctResult")
                    );

            对于流式查询，计算查询结果所需的状态可能会无限增长，具体取决于聚合类型和不同分组键的数量。
            请提供具有有效保留间隔的查询配置，以防止状态大小过大。有关详细信息，请参阅查询配置。
         */

        // 输出
        tableEnv.toRetractStream(resTable, Row.class).print();

        env.execute("D17_TableApi_Aggregation_Distinct");
    }
}
