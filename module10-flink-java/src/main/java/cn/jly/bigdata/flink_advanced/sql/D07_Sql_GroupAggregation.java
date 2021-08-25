package cn.jly.bigdata.flink_advanced.sql;

import cn.jly.bigdata.flink_advanced.datastream.beans.Order;
import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 分组聚合
 * <p>
 * 对于流式查询，重要的是要了解 Flink 运行永不终止的连续查询。相反，他们根据其输入表的更新来更新他们的结果表。
 * <p>
 * 动态表和连续查询的概念很重要
 *
 * @author jilanyang
 * @date 2021/8/25 17:22
 */
public class D07_Sql_GroupAggregation {
    @SneakyThrows
    public static void main(String[] args) {
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

        // distinct聚合在应用聚合函数之前删除重复值。以下示例计算不同 order_id 的数量，而不是 Orders 表中的总行数。
        Table distinctCountTable = tableEnv.sqlQuery(
                "select count(distinct orderId) from tbl_order"
        );
        tableEnv.toRetractStream(distinctCountTable, Row.class).print("distinctCountTable");

        /*
            GROUPING SETS 分组集-多列分组
            分组集允许比标准 GROUP BY 描述的更复杂的分组操作。行按每个指定的分组集单独分组，并且为每个组计算聚合，就像简单的 GROUP BY 子句一样。

            GROUPING SETS 的每个子列表都可以指定零个或多个列或表达式，其解释方式与直接在 GROUP BY 子句中使用的方式相同。空分组集意味着所有行都聚合到一个组中，即使不存在输入行也会输出。
            对分组列或表达式的引用被替换为结果行中的空值，用于分组集中没有出现这些列的集合。

            方法相同于 group by column1, column2
         */
        Table groupingSetsTable = tableEnv.sqlQuery(
                "SELECT supplier_id, rating, COUNT(*) AS total\n" +
                        "FROM (VALUES\n" +
                        "    ('supplier1', 'product1', 4),\n" +
                        "    ('supplier1', 'product2', 3),\n" +
                        "    ('supplier2', 'product3', 3),\n" +
                        "    ('supplier2', 'product4', 4))\n" +
                        "AS Products(supplier_id, product_id, rating)\n" +
                        "GROUP BY GROUPING SETS ((supplier_id, rating), (supplier_id), ())"
        );
        tableEnv.toRetractStream(groupingSetsTable, Row.class).print("groupingSetsTable");

        /*
            ROLLUP
            以下的方式等价于上面
         */
        Table rollupTable = tableEnv.sqlQuery(
                "SELECT supplier_id, rating, COUNT(*)\n" +
                        "FROM (VALUES\n" +
                        "    ('supplier1', 'product1', 4),\n" +
                        "    ('supplier1', 'product2', 3),\n" +
                        "    ('supplier2', 'product3', 3),\n" +
                        "    ('supplier2', 'product4', 4))\n" +
                        "AS Products(supplier_id, product_id, rating)\n" +
                        "GROUP BY ROLLUP (supplier_id, rating)"
        );
        tableEnv.toRetractStream(rollupTable, Row.class).printToErr("rollupTable");

        /*
            CUBE
            CUBE 是用于指定常见类型的分组集的速记符号。它代表给定的列表及其所有可能的子集——幂集。
            例如，以下两个查询是等效的。

            // 1.
            SELECT supplier_id, rating, product_id, COUNT(*)
            FROM (VALUES
                ('supplier1', 'product1', 4),
                ('supplier1', 'product2', 3),
                ('supplier2', 'product3', 3),
                ('supplier2', 'product4', 4))
            AS Products(supplier_id, product_id, rating)
            GROUP BY CUBE (supplier_id, rating, product_id)

            // 2.
            SELECT supplier_id, rating, product_id, COUNT(*)
            FROM (VALUES
                ('supplier1', 'product1', 4),
                ('supplier1', 'product2', 3),
                ('supplier2', 'product3', 3),
                ('supplier2', 'product4', 4))
            AS Products(supplier_id, product_id, rating)
            GROUP BY GROUPING SET (
                ( supplier_id, product_id, rating ),
                ( supplier_id, product_id         ),
                ( supplier_id,             rating ),
                ( supplier_id                     ),
                (              product_id, rating ),
                (              product_id         ),
                (                          rating ),
                (                                 )
            )

         */

        /*
            !!!!!!!!!!!!!! HAVING
            HAVING 消除不满足条件的组行。 HAVING 与 WHERE 不同：WHERE 在 GROUP BY 之前过滤单个行，而 HAVING 过滤由 GROUP BY 创建的组行。
            条件中引用的每个列必须明确引用分组列，除非它出现在聚合函数中。

            即使没有 GROUP BY 子句，HAVING 的存在也会将查询转换为分组查询。这与查询包含聚合函数但没有 GROUP BY 子句时发生的情况相同。
            查询将所有选定的行视为一个组，SELECT 列表和 HAVING 子句只能从聚合函数中引用表列。如果 HAVING 条件为真，则此类查询将发出单行，如果不为真，则将发出零行。

         */
        Table havingTable = tableEnv.sqlQuery(
                "select userId, sum(money) as sum_money from tbl_order group by userId having sum(money) > 100"
        );
        tableEnv.toRetractStream(havingTable, Row.class).printToErr("havingTable");

        env.execute("D07_Sql_GroupAggregation");
    }
}
