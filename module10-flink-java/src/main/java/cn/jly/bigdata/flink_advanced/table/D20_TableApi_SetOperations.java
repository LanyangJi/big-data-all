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
 * table api中的集合操作
 *
 * @author jilanyang
 * @createTime 2021/8/16 10:14
 */
public class D20_TableApi_SetOperations {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1); // 为了方便查看，这边并行度设置为1
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // source - 订单流1
        SingleOutputStreamOperator<Order> orderDS = env.socketTextStream("linux01", 9999)
                .flatMap(
                        new FlatMapFunction<String, Order>() {
                            @Override
                            public void flatMap(String s, Collector<Order> collector) throws Exception {
                                String[] fields = s.split(",");
                                String orderId = fields[0];
                                String userId = fields[1];
                                long createTime = Long.parseLong(fields[2]);
                                double money = Double.parseDouble(fields[3]);

                                collector.collect(new Order(orderId, userId, createTime, money));
                            }
                        }
                );

        // 订单流2 -- 这边只是为了演示集合操作，并没有严格意义上的业务说明或限制
        SingleOutputStreamOperator<Order> orderDS2 = env.socketTextStream("linux01", 8888)
                .flatMap(
                        new FlatMapFunction<String, Order>() {
                            @Override
                            public void flatMap(String s, Collector<Order> collector) throws Exception {
                                String[] fields = s.split(",");
                                String orderId = fields[0];
                                String userId = fields[1];
                                long createTime = Long.parseLong(fields[2]);
                                double money = Double.parseDouble(fields[3]);

                                collector.collect(new Order(orderId, userId, createTime, money));
                            }
                        }
                );

        // dataStream -> table
        Table leftOrder = tableEnv.fromDataStream(orderDS);
        Table rightOrder = tableEnv.fromDataStream(orderDS2);

        /*
            union(批处理，去重和排序)/unionAll（批处理、流处理，不去重不排序）, 类似于 SQL UNION ALL 子句。联合两张表。两个表必须具有相同的字段类型。

            union和union all的区别是,union会自动压缩多个结果集合中的重复结果，而union all则将所有的结果全部显示出来，不管是不是重复。
            Union：对两个结果集进行并集操作，不包括重复行，同时进行默认规则的排序；
            UNION在进行表链接后会筛选掉重复的记录，所以在表链接后会对所产生的结果集进行排序运算，删除重复的记录再返回结果。
            实际大部分应用中是不会产生重复的记录，最常见的是过程表与历史表UNION


            Union All：对两个结果集进行并集操作，包括重复行，不进行排序；
            如果返回的两个结果集中有重复的数据，那么返回的结果集就会包含重复的数据了。
            ————————————————
            原文链接：https://blog.csdn.net/u010931123/article/details/82425580
         */
        // The UNION operation on two unbounded tables is currently not supported.
        // union目前不支持流处理环境
        //Table unionTable = orderTable1.union(orderTable2);

        Table unionAllTable = leftOrder.unionAll(rightOrder);

        // 输出
        tableEnv.toAppendStream(unionAllTable, Row.class).print("unionAll");

        /*
            todo intersect,求交集，只支持批处理，去重
            类似于 SQL INTERSECT 子句。 Intersect 返回存在于两个表中的记录。如果一个记录在一个或两个表中出现不止一次，它只返回一次，
            即结果表没有重复的记录。两个表必须具有相同的字段类型。
                Table left = tableEnv.from("orders1");
                Table right = tableEnv.from("orders2");
                left.intersect(right);

            todo intersectAll,求交集，只支持批处理，不去重
            类似于 SQL INTERSECT ALL 子句。 IntersectAll 返回两个表中都存在的记录。如果一个记录在两个表中出现多次，
            则返回它在两个表中出现的次数，即结果表可能有重复的记录。两个表必须具有相同的字段类型。
                Table left = tableEnv.from("orders1");
                Table right = tableEnv.from("orders2");
                left.intersectAll(right);

            todo minus,求差集，只支持批处理，去重
            类似于 SQL EXCEPT 子句。减号返回左表中不存在于右表中的记录。左表中的重复记录只返回一次，即删除重复项。两个表必须具有相同的字段类型。
                Table left = tableEnv.from("orders1");
                Table right = tableEnv.from("orders2");
                left.minus(right);

           todo minusAll,求差集，只支持批处理，不去重
           类似于 SQL EXCEPT ALL 子句。 MinusAll 返回右表中不存在的记录。在左表中出现 n 次，在右表中出现 m 次的记录返回 (n - m) 次，
           即删除与右表中存在的重复项一样多。两个表必须具有相同的字段类型。
                Table left = tableEnv.from("orders1");
                Table right = tableEnv.from("orders2");
                left.minusAll(right);

         */

        /*
            in 支持批、流处理，类似于sql中的in，如果子查询中存在便返回true，子查询中必须包含与查询列相同的数据类型
                类似于SQL IN子句。如果给定表子查询中存在表达式，则In返回true。子查询表必须由一列组成。此列必须与表达式具有相同的数据类型。


         */
        Table inTable = leftOrder.where($("userId").in(rightOrder))
                .select($("orderId"), $("userId"), $("money"));

        // 打印输出
        tableEnv.toAppendStream(inTable, Row.class).print("in");

        env.execute("D20_TableApi_SetOperations");
    }
}
