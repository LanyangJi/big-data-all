package cn.jly.bigdata.flink_advanced.sql;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author jilanyang
 * @date 2021/8/25 17:58
 */
public class D08_Sql_GroupByWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Order> ds = env.socketTextStream("linux01", 9999)
                .map(
                        new MapFunction<String, Order>() {
                            @Override
                            public Order map(String value) throws Exception {
                                String[] split = value.split(",");
                                return new Order(split[0], split[1], Long.parseLong(split[2]), Double.parseDouble(split[3]));
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        // 指定生成时间戳和水印的规则——允许数据迟到3秒
                        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Order>() {
                                            @Override
                                            public long extractTimestamp(Order element, long recordTimestamp) {
                                                return element.getCreateTime();
                                            }
                                        }
                                )
                );

        // 从dataStream中创建视图
        tableEnv.createTemporaryView("tbl_order", ds, $("orderId"), $("userId"), $("createTime").rowtime(), $("money"));

        // group by window

        env.execute("D08_Sql_GroupByWindow");
    }
}
