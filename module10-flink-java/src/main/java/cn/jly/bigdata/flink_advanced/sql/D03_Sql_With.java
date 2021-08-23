package cn.jly.bigdata.flink_advanced.sql;

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
 * with语句
 *
 * @author jilanyang
 * @date 2021/8/23 20:14
 */
public class D03_Sql_With {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> ds = env.socketTextStream("linux01", 9999)
                .map(
                        new MapFunction<String, Tuple3<String, Double, Double>>() {
                            @Override
                            public Tuple3<String, Double, Double> map(String value) throws Exception {
                                String[] fields = value.split(",");
                                return Tuple3.of(fields[0], Double.parseDouble(fields[1]), Double.parseDouble(fields[2]));
                            }
                        }
                );

        // 直接通过dataStream创建临时视图
        tableEnv.createTemporaryView("tbl_order", ds, $("userId"), $("fruit_pay"), $("other_pay"));
        Table resTable = tableEnv.sqlQuery(
                "with tbl_sum_pay as( " +
                        "    select  " +
                        "        userId, " +
                        "        fruit_pay + other_pay as sum_pay " +
                        "    from tbl_order " +
                        ") " +
                        "select  " +
                        "    userId, " +
                        "    avg(sum_pay) as avg_pay " +
                        "from tbl_sum_pay " +
                        "group by userId"
        );

        tableEnv.toRetractStream(resTable, Row.class).print();

        env.execute("D03_Sql_With");
    }
}
