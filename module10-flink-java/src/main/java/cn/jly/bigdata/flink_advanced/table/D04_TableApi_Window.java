package cn.jly.bigdata.flink_advanced.table;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.table
 * @class D04_TableApi_Window
 * @date 2021/8/2 22:31
 */
public class D04_TableApi_Window {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "localhost");
        int port = tool.getInt("port", 9999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1); // 为了方便观察
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // source
        SingleOutputStreamOperator<Order> orderDs = env.socketTextStream(host, port)
                .map(new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Order(fields[0], fields[1], Long.parseLong(fields[2]), Double.parseDouble(fields[3]));
                    }
                });

        // 事件时间 + watermark
        SingleOutputStreamOperator<Order> orderWithWatermarkDs = orderDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                            @Override
                            public long extractTimestamp(Order element, long recordTimestamp) {
                                return element.getCreateTime();
                            }
                        })
        );

        // 创建表
        Table resTable = tableEnv.fromDataStream(
                        orderWithWatermarkDs,
                        $("orderId"),
                        $("userId"),
                        $("createTime").rowtime().as("rowtime"),
                        $("money")
                )
                .window(
                        Tumble.over(lit(5).seconds())
                                .on($("rowtime"))
                                .as("w")
                )
                .groupBy($("w"), $("userId"))
                .select(
                        $("userId"),
                        $("orderId").count().as("order_count"),
                        $("money").max().as("max_money"),
                        $("money").min().as("min_money")
                );

        // 转换输出
        DataStream<Tuple2<Boolean, Row>> resDs = tableEnv.toRetractStream(resTable, Row.class);
        resDs.print();

        env.execute("D04_TableApi_Window");
    }
}
