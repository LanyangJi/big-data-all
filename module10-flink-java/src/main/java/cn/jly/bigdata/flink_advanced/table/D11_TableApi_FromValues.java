package cn.jly.bigdata.flink_advanced.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @author jilanyang
 * @createTime 2021/8/12 10:25
 */
public class D11_TableApi_FromValues {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从内存中数据构造表
        Table table = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("orderId", DataTypes.STRING()),
                        DataTypes.FIELD("userId", DataTypes.STRING()),
                        DataTypes.FIELD("money", DataTypes.DECIMAL(10, 2))
                ),
                row("order-1", "user-1", 23.3),
                row("order-2", "user-1", 44.4)
        );

        table.printSchema();
        
        // 输出
        DataStream<Row> resDS = tableEnv.toAppendStream(table.select($("*")), Row.class); // *是通配符，表示查询所有列
        resDS.print();

        env.execute("D11_TableApi_FromValues");
    }
}
