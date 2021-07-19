package cn.jly.bigdata.flink.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * explain table
 * Table API提供了一种机制来解释计算Table的逻辑和优化查询计划。这是通过Table.explain()方法或StatementSet.explain()方法完成的。
 * Table.explain()返回Table的计划。StatementSet.explain()返回多个接收器的计划。它返回一个描述三个计划的String
 * -> 关系查询的抽象语法树，即未优化的逻辑查询计划、优化的逻辑查询计划和物理执行计划
 * TableEnvironment.explainSql()和TableEnvironment.executeSql()支持执行EXPLAIN语句来获取计划，请参考EXPLAIN页面
 *
 * @author jilanyang
 * @date 2021/7/19 16:51
 */
public class D06_TableApi_ExplainTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
        DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello"));

        // explain Table API
        Table table1 = tEnv.fromDataStream(stream1, $("count"), $("word"));
        Table table2 = tEnv.fromDataStream(stream2, $("count"), $("word"));
        Table table = table1
                .where($("word").like("F%"))
                .unionAll(table2);

        System.out.println(table.explain());
    }
}
