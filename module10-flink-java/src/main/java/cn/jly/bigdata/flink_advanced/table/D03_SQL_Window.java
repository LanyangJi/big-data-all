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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用Flink SQL来统计5秒内 每个用户的 订单总数、订单的最大金额、订单的最小金额
 * 也就是每隔5秒统计最近5秒的每个用户的订单总数、订单的最大金额、订单的最小金额
 * 上面的需求使用流处理的Window的基于时间的滚动窗口就可以搞定!
 * 那么接下来使用FlinkTable&SQL-API来实现
 * <p>
 * 使用事件时间+Watermark+Flink SQL和Table中的window
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.table
 * @class D03_SQL_Window
 * @date 2021/8/2 22:03
 */
public class D03_SQL_Window {
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

        // 创建视图, 指定列的时候要指定哪一列是时间
        tableEnv.createTemporaryView(
                "tbl_order",
                orderWithWatermarkDs,
                $("orderId"),
                $("userId"),
                $("createTime").rowtime(),
                $("money")
        );

        // sql查询
        String sql = "select userId, count(*) as order_count, max(money) as max_money, min(money) as min_money from tbl_order" +
                " group by userId, tumble(createTime, interval '5' second)";
        Table resTable = tableEnv.sqlQuery(sql);

        // 转换输出
        DataStream<Tuple2<Boolean, Row>> resDs = tableEnv.toRetractStream(resTable, Row.class);
        resDs.print();

        env.execute("D03_SQL_Window");
    }
}
