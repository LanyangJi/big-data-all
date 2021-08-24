package cn.jly.bigdata.flink_advanced.sql;

import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * SELECT 语句和 VALUES 语句是使用 TableEnvironment 的 sqlQuery() 方法指定的。该方法将 SELECT 语句（或 VALUES 语句）的结果作为表返回。
 * Table 可以在后续的 SQL 和 Table API 查询中使用，转换为 DataStream，或写入 TableSink。 SQL 和 Table API 查询可以无缝混合，并进行整体优化并转换为单个程序。
 * <p>
 * 为了访问 SQL 查询中的表，它必须在 TableEnvironment 中注册。可以从 TableSource、Table、CREATE TABLE 语句、DataStream 注册表。
 * 或者，用户也可以在 TableEnvironment 中注册目录以指定数据源的位置。
 * <p>
 * 为方便起见，Table.toString() 自动在其 TableEnvironment 中以唯一名称注册该表并返回该名称。因此，Table 对象可以直接内联到 SQL 查询中，如下面的示例所示。
 *
 * @author jilanyang
 * @date 2021/8/23 19:05
 */
public class D02_Sql_SqlQuery {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> ds = env.socketTextStream("linux01", 9999)
                .map(
                        new MapFunction<String, Tuple3<Long, String, Integer>>() {
                            @Override
                            public Tuple3<Long, String, Integer> map(String s) throws Exception {
                                String[] split = s.split(",");
                                return Tuple3.of(Long.parseLong(split[0]), split[1], Integer.parseInt(split[2]));
                            }
                        }
                );


        // 方式一：
        // 带有内联（未注册）表的 SQL 查询
        Table table = tableEnv.fromDataStream(ds, $("userId"), $("name"), $("amount"));
        Table resTable01 = tableEnv.sqlQuery(
                "select sum(amount) as sum_amount from " + table + " group by name"
        );
        tableEnv.toRetractStream(resTable01, Row.class).print("resTable01");


        // 方式二：
        // 带有注册表的 SQL 查询
        // 将 DataStream 注册为视图“订单”
        tableEnv.createTemporaryView("tbl_order", ds, $("userId"), $("name"), $("amount"));
         // 对表运行 SQL 查询并将结果作为新表检索
        Table resTable02 = tableEnv.sqlQuery(
                "select sum(amount) as sum_amount from tbl_order group by name"
        );
        tableEnv.toRetractStream(resTable02, Row.class).printToErr("resTable02");

        env.execute("D02_Sql_SqlQuery");
    }
}
