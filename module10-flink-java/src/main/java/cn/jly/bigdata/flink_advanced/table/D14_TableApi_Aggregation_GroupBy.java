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
 * 类似于 SQL GROUP BY 子句。使用以下正在运行的聚合运算符对分组键上的行进行分组，以按组聚合行。
 *
 * 对于流式查询，计算查询结果所需的状态可能会无限增长，具体取决于聚合类型和不同分组键的数量。
 * 请提供具有有效保留间隔的查询配置，以防止状态大小过大。有关详细信息，请参阅查询配置。
 *
 * @author jilanyang
 * @createTime 2021/8/13 14:32
 */
public class D14_TableApi_Aggregation_GroupBy {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
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

        // DataStream -> Table
        Table orderTable = tableEnv.fromDataStream(orderDS);

        // groupBy
        Table aggTable = orderTable.groupBy($("userId"))
                .select($("userId"), $("money").sum().as("sum_money"));

        // 输出
        tableEnv.toRetractStream(aggTable, Row.class)
                .print();

        env.execute("D14_TableApi_Aggregation_GroupBy");
    }
}
