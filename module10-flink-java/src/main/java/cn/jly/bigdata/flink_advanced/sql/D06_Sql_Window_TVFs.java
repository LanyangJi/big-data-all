package cn.jly.bigdata.flink_advanced.sql;

import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Windowing table-valued functions (Windowing TVFs)
 * 窗口化表值函数（窗口化 TVF）
 * <p>
 * Windows 是处理无限流的核心。 Windows 将流拆分为有限大小的“桶”，我们可以对其进行计算。本文档重点介绍如何在 Flink SQL 中执行窗口化以及程序员如何从其提供的功能中获得最大收益。
 * <p>
 * Apache Flink 提供了几个窗口表值函数 (TVF) 来将表的元素划分为窗口，包括：
 * 1. Tumble Windows
 * 2. Hop Windows 跳窗
 * 3. Cumulate Windows
 * 4. Session Windows (即将支持)
 * <p>
 * Windowing TVF 是 Flink 定义的多态表函数（简称 PTF）。 PTF 是 SQL 2016 标准的一部分，是一种特殊的表函数，但可以将表作为参数。 PTF 是改变表格形状的强大功能。因为 PTF 在语义上类似于表，所以它们的调用发生在 SELECT 语句的 FROM 子句中。
 * 窗口化 TVF 替代了传统的分组窗口函数。窗口化 TVF 更符合 SQL 标准，并且更强大地支持复杂的基于窗口的计算，例如窗口 TopN，窗口连接。但是，分组窗口函数只能支持窗口聚合。
 * <p>
 * Apache Flink 提供了 3 个内置的窗口 TVF：TUMBLE、HOP 和 CUMULATE。
 * 窗口化TVF的返回值是一个新的关系，包括原始关系的所有列，以及额外的3列，分别名为“window_start”、“window_end”、“window_time”，表示分配的窗口。
 * “window_time”字段是TVF开窗后窗口的时间属性，可用于后续的基于时间的操作，例如聚合上的另一个窗口 TVF 或间隔连接。
 * ！！！！！！！！！！！！！！window_time 的值总是等于 window_end - 1ms。
 *
 * @author jilanyang
 * @date 2021/8/25 16:27
 */
public class D06_Sql_Window_TVFs {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Tuple3<String, Double, Long>> ds = env.socketTextStream("linux01", 9999)
                .map(
                        new MapFunction<String, Tuple3<String, Double, Long>>() {
                            @Override
                            public Tuple3<String, Double, Long> map(String value) throws Exception {
                                String[] split = value.split(",");
                                return Tuple3.of(split[0], Double.parseDouble(split[1]), Long.parseLong(split[2]));
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        // 指定生成时间戳和水印的规则——允许数据迟到3秒
                        WatermarkStrategy.<Tuple3<String, Double, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple3<String, Double, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple3<String, Double, Long> element, long recordTimestamp) {
                                                return element.f2;
                                            }
                                        }
                                )
                );

        // 程序运行报错
        // 此功能当前版本不支持：Currently Flink doesn't support individual window table-valued function TUMBLE(time_col=[ts], size=[5 s]).
        // 官网示例是在flink sql client中测试的，todo 需要去验证一下
        // 根据dataStream直接创建视图, 方式一
        tableEnv.createTemporaryView("tbl_order", ds, $("userId"), $("price"), $("ts").rowtime());

//        Table table = tableEnv.sqlQuery(
//                "select * from table(tumble(table tbl_order, descriptor(ts), interval '5' seconds))"
//        );
//        tableEnv.toAppendStream(table, Row.class).print();

        // 方式二
        Table table2 = tableEnv.sqlQuery(
                "select * from table(tumble(DATA =>table tbl_order, TIMECOL => descriptor(ts), SIZE => interval '5' seconds))"
        );
        tableEnv.toAppendStream(table2, Row.class).print();

        env.execute("D06_Sql_Window_TVFs");
    }
}
