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
import static org.apache.flink.table.api.Expressions.uuid;

/**
 * 对列的操作，涉及addColumns,addOrReplaceColumn,dropColumns,renameColumns
 *
 * @author jilanyang
 * @createTime 2021/8/13 13:50
 */
public class D13_TableApi_ModifyColumns {
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

        // addColumns - 执行字段添加操作。如果添加的字段已经存在，它将抛出异常。
        Table newOrderTable = orderTable.addColumns(uuid().as("page_id"), $("user_id").lowerCase().as("lower_name"));
        newOrderTable.printSchema();

        // 输出
        tableEnv.toAppendStream(newOrderTable, Row.class).print("newOrderTable");

        /*
         addOrReplaceColumns - 执行字段添加操作。如果添加的列名称与现有列名称相同，则现有字段将被替换。
                此外，如果添加的字段具有重复的字段名称，则使用最后一个。
         */
        Table newOrderTable2 = orderTable.addOrReplaceColumns(uuid().as("order_id"));
        newOrderTable2.printSchema();

        // 输出
        tableEnv.toAppendStream(newOrderTable2, Row.class).printToErr("newOrderTable2");


        // dropColumns
        Table newOrderTable3 = orderTable.dropColumns($("order_id"), $("create_time"));
        newOrderTable3.printSchema();

        // 输出
        tableEnv.toAppendStream(newOrderTable3, Row.class).print("newOrderTable3");

        // rename columns
        Table newOrderTable4 = orderTable.renameColumns($("order_id").as("o_id"), $("create_time").as("event_time"));
        newOrderTable4.printSchema();

        // 输出
        tableEnv.toAppendStream(newOrderTable4, Row.class).printToErr("newOrderTable4");

        env.execute("D13_TableApi_ModifyColumns");
    }
}
