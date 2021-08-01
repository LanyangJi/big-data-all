package cn.jly.bigdata.flink_advanced.datastream.c09_4cores_checkpoint;

import cn.jly.bigdata.flink_advanced.utils.StreamWordCountUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 自动重启策略与恢复：
 * 1. 默认重启策略
 * 2. 无重启策略
 * 3. 固定延迟重启策略——开发中使用
 * 4. 失败率重启策略——开发偶尔使用
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c09_4cores_checkpoint
 * @class D01_Checkpoint
 * @date 2021/8/1 19:12
 */
public class D02_RestartStrategy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1); // 并行度设为1方便观察

        // TODO checkpoint一些常用的配置
        //===========类型1:必须参数=============
        // 每隔1s执行一次checkpoint，一般设置为秒级或者分钟，毫秒级（比如十几毫秒）比较影响性能，毕竟有的状态比较大
        env.enableCheckpointing(1000);
        // 方式一：旧的 FsStateBackend 相当于使用 HashMapStateBackend 和 FileSystemCheckpointStorage。
        env.setStateBackend(new HashMapStateBackend());
        if (SystemUtils.IS_OS_WINDOWS) { // 根据运行程序的操作系统类型设置checkpoint存储目录
            env.getCheckpointConfig().setCheckpointStorage("file:///d:/JLY/test/ckp");
        } else {
            env.getCheckpointConfig().setCheckpointStorage("hdfs://linux01:8020/flink-checkpoint/checkpoing");
        }
        // 方式二：使用rocksDB作为状态后端
        // env.setStateBackend(new EmbeddedRocksDBStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs://linux01:8020/flink-checkpoint/checkpoing");

        //===========类型2:建议参数===========
        //设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms
        // (为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了)
        //如:高速公路上,每隔1s关口放行一辆车,但是规定了两车之前的最小车距为500m
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);//默认是0
        //设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是  false不是
        // env.getCheckpointConfig().setFailOnCheckpointingErrors(false);//默认是true，该api过期了，推荐使用下面的次数约定
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);//默认值为0，表示不容忍任何检查点失败
        //设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true,当作业被取消时，删除外部的checkpoint(默认值)
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false,当作业被取消时，保留外部的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //===========类型3:直接使用默认的即可===============
        //设置checkpoint的执行模式为EXACTLY_ONCE(默认)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
        env.getCheckpointConfig().setCheckpointTimeout(60000);//默认10分钟
        //设置同一时间有多少个checkpoint可以同时执行; 这边如何设置为1的话是和配置项setMinPauseBetweenCheckpoints冲突的，二者取其一
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);//默认为1

        /*
            自动重启策略与恢复：
            1. 默认重启策略
            2. 无重启策略
            3. 固定延迟重启策略——开发中使用
            4. 失败率重启策略——开发偶尔使用

            注意：
            1. 配置了checkpoint的情况下不做任务配置：默认是无限重启并自动恢复，可以解决小问题，但是可能会隐藏真正的bug
            2. 单独配置无重启策略——RestartStrategies.noRestart()
            3. 固定延迟重启 - RestartStrategies.fixedDelayRestart(2, Time.seconds(5))
            4. 失败率重启 - 5分钟内最多失败3次，超过3则程序退出；每次重启的时间间隔为3s
                RestartStrategies.failureRateRestart(
                    3, // 每个测量阶段的最大失败次数
                    Time.minutes(5), // 失败率测量的时间间隔
                    Time.seconds(3) // 两次连续重启的时间间隔
                )
         */
        // 设置失败重启策略：固定延迟策略：程序出现一场后，重启2次，每次延迟5秒重启；超过2次后，程序退出
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(5)));

        DataStreamSource<String> lineDs = env.socketTextStream("linux01", 9999);
        lineDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            // 在这边模拟一个异常
                            if ("bug".equalsIgnoreCase(word)) {
                                throw new RuntimeException("bug出现了...");
                            }
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        env.execute("D02_RestartStrategy");
    }
}
