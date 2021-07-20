package cn.jly.bigdata.flink.table;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * 我们建议在切换到 Table API 之前尽早设置 DataStream API 中的所有配置选项
 * <p>
 * The TableEnvironment will adopt all configuration options from the passed StreamExecutionEnvironment.
 * However, it cannot be guaranteed that further changes to the configuration of StreamExecutionEnvironment
 * are propagated to the StreamTableEnvironment after its instantiation.
 * Also, the reverse propagation of options from Table API to DataStream API is not supported.
 *
 * @author lanyangji
 * @date 2021/7/9 13:58
 * @packageName cn.jly.bigdata.flink.table
 * @className D02_Configuration
 */
public class D99_Configuration {
    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 早点设定配置，在转换成table api之前
        env.setMaxParallelism(256);
        //env.getConfig().addDefaultKryoSerializer(MyCustomType.class, CustomKryoSerializer.class);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 然后再切换到table api
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // set configuration early
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Europe/Berlin"));

        // start defining your pipelines in both APIs...
        // ...
        // env.execute("D02_Configuration");
    }
}
