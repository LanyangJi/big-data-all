package cn.jly.bigdata.flink.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Instant;
import java.util.StringJoiner;

/**
 * @author lanyangji
 * @date 2021/7/9 15:04
 * @packageName cn.jly.bigdata.flink.table
 * @className D03_FromDataStream
 */
public class D03_FromDataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        DataStream<User> dataStream = env.fromElements(
                new User("Alice", 4, Instant.ofEpochMilli(1000)),
                new User("Bob", 6, Instant.ofEpochMilli(1001)),
                new User("Alice", 10, Instant.ofEpochMilli(1002))
        );

        // 1. 自动派生所有列
        Table table = tableEnv.fromDataStream(dataStream);
        table.printSchema();

        // 2自动派生所有物理列
        // 但添加计算列（在本例中用于创建 proc_time 属性列）
        Table table2 = tableEnv.fromDataStream(
                dataStream,
                Schema.newBuilder().columnByExpression("proc_time", "PROCTIME()").build()
        );
        table2.printSchema();

        // 3. 自动派生所有物理列
        // 但添加计算列（在本例中用于创建行时间属性列）
        // 和自定义水印策略
        Table table3 =
                tableEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
                                .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
                                .build());
        table3.printSchema();

        // 打印
        dataStream.printToErr();

        env.execute("D03_FromDataStream");
    }

    public static class User {
        public String name;
        public Integer score;
        public Instant event_time;

        public User() {
        }

        public User(String name, Integer score, Instant event_time) {
            this.name = name;
            this.score = score;
            this.event_time = event_time;
        }

        @Override
        public String toString() {
            return "User{" +
                    "name='" + name + '\'' +
                    ", score=" + score +
                    ", event_time=" + event_time +
                    '}';
        }
    }
}
