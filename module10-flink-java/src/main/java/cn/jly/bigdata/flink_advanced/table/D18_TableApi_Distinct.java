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

/**
 * 类似于 SQL DISTINCT 子句。返回具有不同值组合的记录。
 * <p>
 * 对于流式查询，计算查询结果所需的状态可能会无限增长，具体取决于聚合类型和不同分组键的数量。请提供具有有效保留间隔的查询配置，
 * 以防止状态大小过大。有关详细信息，请参阅查询配置。
 *
 * @author jilanyang
 * @createTime 2021/8/13 16:06
 */
public class D18_TableApi_Distinct {
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

        tableEnv.toRetractStream(
                orderTable.distinct(), // 去重，注意，这个挺强大，即使Order类没有重写equals和hashcode。当然其实从流转成表的时候，已经和Order本身类没有多大关系了
                Row.class
        )
                .print();

        env.execute("D18_TableApi_Distinct");
    }
}
