package cn.jly.bigdata.flink_advanced.table;

import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 类似于 SQL OVER 子句。基于前一行和后一行的窗口（范围），为每一行计算过窗口聚合。有关更多详细信息，请参阅窗口部分。
 * 类似与hive中的窗口函数
 *
 * 所有聚合必须在同一个窗口中定义，即相同的分区、排序和范围。目前，仅支持具有 PRECEDING（未绑定和有界）到 CURRENT ROW 范围的窗口。
 * 尚不支持带有 FOLLOWING 的范围。 ORDER BY 必须在单个时间属性上指定。
 *
 * @author jilanyang
 * @createTime 2021/8/13 14:32
 */
public class D16_TableApi_Aggregation_Window_Over {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1); // 为了方便查看，这边并行度设置为1
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.execute("D16_TableApi_Aggregation_Window_Over");
    }
}
