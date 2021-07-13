package cn.jly.bigdata.flink.datastream.c03_transform;

import cn.hutool.core.util.StrUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lanyangji
 * @date 2021/7/8 19:29
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D17_StateBackend
 */
public class D17_StateBackend {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 文件系统状态后端配置
        // 此方式过时了
        // env.setStateBackend(new FsStateBackend(""));
        env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:///checkpoints");
        env.getCheckpointConfig().setCheckpointStorage("file:///e:/flink-fs-checkpoint");

        //  checkpoint其他配置
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        // 2. env重启策略
        // 固定延迟重启，隔一段尝试重启一次，总共尝试3次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100000));

        env.readTextFile("input/word.txt")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        if (StrUtil.isNotBlank(value)) {
                            for (String word : value.split(" ")) {
                                out.collect(Tuple2.of(word, 1L));
                            }
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1)
                .printToErr();

        env.execute("D17_StateBackend");
    }
}
