package cn.jly.bigdata.flink.table;

import cn.jly.bigdata.flink.table.beans.RawUser;
import cn.jly.bigdata.flink.table.beans.User;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.time.Instant;

/**
 * @author jilanyang
 * @date 2021/7/20 17:24
 */
public class D10_TableApi_FromDataStream {
    public static void main(String[] args) {
        // 1. 创建dataStream
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 创建dataStream
        DataStream<User> dataStream = env.fromElements(
                new User("Alice", 4, Instant.ofEpochMilli(1000)),
                new User("Bob", 6, Instant.ofEpochMilli(1001)),
                new User("Alice", 10, Instant.ofEpochMilli(1002))
        );

        // 3. fromDataStream
        Table table01 = tableEnv.fromDataStream(dataStream);
        table01.printSchema();

        // 4. 添加一列process time
        Table table02 = tableEnv.fromDataStream(
                dataStream,
                Schema.newBuilder()
                        .columnByExpression("proc_time", "proctime()")
                        .build()
        );
        table02.printSchema();

        // 5. 自动派生所有物理列，但添加计算列(在本例中用于创建rowtime属性列)和自定义水印策略
        Table table03 = tableEnv.fromDataStream(
                dataStream,
                Schema.newBuilder()
                        .columnByExpression("row_time", "cast(event_time as timestamp_ltz(3))")
                        // 水印，允许延迟10秒
                        .watermark("row_time", "row_time - interval '10' second")
                        .build()
        );
        table03.printSchema();

        // 6. 自动派生所有物理列，但访问流记录的时间戳来创建row_time属性列也依赖于在DataStream API中生成的水印
        // 我们假设已经为' dataStream '定义了水印策略(不是本例的一部分)
        DataStream<User> dataStream2 = env.fromElements(
                new User("Alice", 4, Instant.ofEpochMilli(1000)),
                new User("Bob", 6, Instant.ofEpochMilli(1001)),
                new User("Alice", 10, Instant.ofEpochMilli(1002))
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<User>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<User>() {
                            @Override
                            public long extractTimestamp(User user, long l) {
                                return user.getEvent_time().getEpochSecond();
                            }
                        })
        );
        // ！！！ 下面的例子要求要在dataStream已经定义好水印生成策略
        Table table04 =
                tableEnv.fromDataStream(
                        dataStream2,
                        Schema.newBuilder()
                                .columnByMetadata("row_time", "TIMESTAMP(3)")
                                .watermark("row_time", "SOURCE_WATERMARK()")
                                .build()
                );
        table04.printSchema();

        // 7.
        DataStream<User> dataStream3 = env.fromElements(
                new User("Alice", 4, Instant.ofEpochMilli(1000)),
                new User("Bob", 6, Instant.ofEpochMilli(1001)),
                new User("Alice", 10, Instant.ofEpochMilli(1002))
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<User>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<User>() {
                            @Override
                            public long extractTimestamp(User user, long l) {
                                return user.getEvent_time().getEpochSecond();
                            }
                        })
        );

        Table table05 = tableEnv.fromDataStream(
                dataStream3,
                Schema.newBuilder()
                        .column("event_time", "timestamp(3)")
                        .column("name", "string")
                        .column("score", "int")
                        .watermark("event_time", "source_watermark()")
                        .build()
        );
        table05.printSchema();

        System.out.println("-------------------------");
        // 对于对象属性是final的
        DataStream<RawUser> dataStream4 = env.fromElements(
                new RawUser("Alice", 4),
                new RawUser("Bob", 6),
                new RawUser("Alice", 10)
        );

        Table table06 = tableEnv.fromDataStream(dataStream4);
        table06.printSchema();

        Table table07 = tableEnv
                .fromDataStream(
                        dataStream4,
                        Schema.newBuilder()
                                .column("f0", DataTypes.of(RawUser.class))
                                .build())
                .as("user");
        table07.printSchema();

        Table table08 = tableEnv
                .fromDataStream(
                        dataStream4,
                        Schema.newBuilder()
                                .column(
                                        "f0",
                                        DataTypes.STRUCTURED(
                                                RawUser.class,
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("score", DataTypes.INT()))
                                )
                                .build())
                .as("user");
        table08.printSchema();
    }
}
