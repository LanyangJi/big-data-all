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
 * where or filter，相当于sql的where子句
 *
 * @author jilanyang
 * @createTime 2021/8/13 13:50
 */
public class D12_TableApi_Where_Filter {
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

        // DataStream -> table
        Table orderTable = tableEnv.fromDataStream(
                orderDS,
                $("orderId").as("order_id"),
                $("userId").as("user_id"),
                $("createTime").as("create_time"),
                $("money")
        );

        // where
        Table queryTable = orderTable.where(
                $("user_id").isEqual("tom")
        )
                .select(
                        $("order_id").lowerCase().as("lower_order_id"),
                        $("user_id"),
                        $("money")
                );

        // 输出
        tableEnv.toAppendStream(queryTable, Row.class)
                .print();

        // filter
        Table queryTable2 = orderTable.filter(
                $("money").isGreater(200)
        )
                .select(
                        $("order_id").lowerCase().as("lower_order_id"),
                        $("user_id"),
                        $("money")
                );
        tableEnv.toAppendStream(queryTable2, Row.class)
                .printToErr();

        env.execute("D12_TableApi_Where_Filter");
    }
}
