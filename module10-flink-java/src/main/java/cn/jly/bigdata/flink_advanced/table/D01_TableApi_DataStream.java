package cn.jly.bigdata.flink_advanced.table;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 *
 * DataStream和Table的转换
 *
 * 案例一：
 * 将DataStream接收到的数据转换为Table或者view进行统计查询
 *
 * @author jilanyang
 * @date 2021/8/2 13:28
 * @package cn.jly.bigdata.flink_advanced.table
 * @class D01_CreateTableEnvironment
 */
public class D01_TableApi_DataStream {
    public static void main(String[] args) throws Exception {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // source: 读取数据得到DataStream
        DataStream<Order> orderDs = env.socketTextStream("linux01", 9999)
                .map(new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Order(fields[0], fields[1], Long.parseLong(fields[2]), Double.parseDouble(fields[3]));
                    }
                });

        // 将DataStream转换为Table，并注册为视图
        Table orderTable = tableEnv.fromDataStream(orderDs).as("orderId", "userId", "createTime", "money");
        tableEnv.createTemporaryView("orderA", orderTable);

        // 直接将DataStream转换为View
        tableEnv.createTemporaryView("orderB", orderDs,
                $("orderId"), $("userId"), $("createTime").as("createTime"), $("money"));

        // 统计，将orderA中money高于20和orderB中money高于30的数据合并
        // =============== 方式一：sql ===============================================
        Table resTable = tableEnv.sqlQuery(
                "select * from orderA a where a.money > 20" +
                        " union select * from orderB b where b.money > 30"
        );
        // 打印输出
        DataStream<Tuple2<Boolean, Order>> orderQueryDs = tableEnv.toRetractStream(resTable, Order.class);
        orderQueryDs.print();

        env.execute("D01_CreateTableEnvironment");
    }
}
