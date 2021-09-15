package cn.jly.bigdata.flink_advanced.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * from后面可以接values指定的内存数据(用as指定为特定的表schema)
 * <p>
 * (values (1, 23.3), (2, 33.6)) as t(order_id, price)是一个整体，表示table t
 *
 * @author jilanyang
 * @date 2021/8/25 16:20
 */
public class D04_Sql_ValuesAs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.sqlQuery(
                "select order_id, price from (values (1, 23.3), (2, 33.6)) as t(order_id, price)"
        );

        table.printSchema();

        tableEnv.toAppendStream(table, Row.class).print();

        env.execute("D04_Sql_ValuesAs");
    }
}
