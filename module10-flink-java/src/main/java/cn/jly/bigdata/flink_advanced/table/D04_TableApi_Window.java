package cn.jly.bigdata.flink_advanced.table;

import cn.hutool.core.util.ObjectUtil;
import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 使用Flink SQL来统计5秒内 每个用户的 订单总数、订单的最大金额、订单的最小金额
 * 也就是每隔5秒统计最近5秒的每个用户的订单总数、订单的最大金额、订单的最小金额
 * 上面的需求使用流处理的Window的基于时间的滚动窗口就可以搞定!
 * 那么接下来使用FlinkTable&SQL-API来实现
 * <p>
 * 使用事件时间+Watermark+Flink SQL和Table中的window
 * <p>
 * 这里使用table api的方式
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.table
 * @class D04_TableApi_Window
 * @date 2021/8/2 22:31
 */
public class D04_TableApi_Window {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
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
        /*
            RowKind:
                @PublicEvolving
                public enum RowKind {
                    INSERT("+I", (byte)0), // 插入
                    UPDATE_BEFORE("-U", (byte)1), // 更新前
                    UPDATE_AFTER("+U", (byte)2), // 更新后
                    DELETE("-D", (byte)3); // 删除
               }
         */
        DataStream<Tuple2<Boolean, Row>> resDs = tableEnv.toRetractStream(resTable, Row.class);
        resDs.print("转换成row");

        // 自定义输出的实体类型
        DataStream<Row> rowDS = tableEnv.toChangelogStream(resTable);
        rowDS.flatMap(new FlatMapFunction<Row, OrderStatistic>() {
            @Override
            public void flatMap(Row row, Collector<OrderStatistic> collector) throws Exception {
                RowKind kind = row.getKind();
                if (ObjectUtil.notEqual(kind, RowKind.DELETE) && ObjectUtil.notEqual(kind, RowKind.UPDATE_BEFORE)) {
                    String userId = row.getFieldAs("userId");
                    Long order_count = row.getFieldAs("order_count");
                    Double max_money = row.getFieldAs("max_money");
                    Double min_money = row.getFieldAs("min_money");
                    collector.collect(new OrderStatistic(userId, order_count, max_money, min_money));
                }
            }
        })
                .printToErr("自定义转换");

        env.execute("D04_TableApi_Window");
    }

    /**
     * 订单统计类
     */
    public static class OrderStatistic {
        public String userId;
        public Long order_count;
        public Double max_money;
        public Double min_money;

        public OrderStatistic(String userId, Long order_count, Double max_money, Double min_money) {
            this.userId = userId;
            this.order_count = order_count;
            this.max_money = max_money;
            this.min_money = min_money;
        }

        public OrderStatistic() {
        }
    }
}
