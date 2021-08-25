package cn.jly.bigdata.flink_advanced.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 如果指定了 SELECT DISTINCT，将从结果集中删除所有重复行（每组重复项中保留一行）。
 *
 * @author jilanyang
 * @date 2021/8/25 16:20
 */
public class D05_Sql_Distinct {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // distinct加载首位，会对所有列进行去重
        Table table = tableEnv.sqlQuery(
                "select distinct order_id, price from (values (1, 23.3), (2, 33.6), (1, 23.3)) as t(order_id, price)"
        );

        table.printSchema();

        tableEnv.toRetractStream(table, Row.class).print();

        env.execute("D05_Sql_Distinct");
    }
}
