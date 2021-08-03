package cn.jly.bigdata.flink_advanced.datastream.c09_4cores_checkpoint;

import cn.jly.bigdata.flink_advanced.utils.StreamWordCountUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * state与checkpoint的区别：
 * 	State:
 * 1. 维护/存储的是某一个Operator的运行的状态/历史值,是维护在内存中!
 * 2. 一般指一个具体的Operator的状态(operator的状态表示一些算子在运行的过程中会产生的一些历史结果,
 * 如前面的maxBy底层会维护当前的最大值,也就是会维护一个keyedOperator,这个State里面存放就是maxBy这个Operator中的最大值)
 * 3. State数据默认保存在Java的堆内存中/TaskManage节点的内存中
 * 4. State可以被记录，在失败的情况下数据还可以恢复
 * 	Checkpoint:
 * 1. 某一时刻,Flink中所有的Operator的当前State的全局快照,一般存在磁盘上
 * 2. 表示了一个Flink Job在一个特定时刻的一份全局状态快照，即包含了所有Operator的状态，可以理解为Checkpoint是把State数据定时持久化存储了，
 * 比如KafkaConsumer算子中维护的Offset状态,当任务重新恢复的时候可以从Checkpoint中获取
 * <p>
 * checkpoint执行流程：
 * 0.Flink的JobManager创建CheckpointCoordinator
 * 1.Coordinator向所有的SourceOperator发送Barrier栅栏(理解为执行Checkpoint的信号)
 * 2.SourceOperator接收到Barrier之后,暂停当前的操作(暂停的时间很短,因为后续的写快照是异步的),并制作State快照, 然后将自己的快照保存到指定的介质中(如HDFS), 一切 ok之后向Coordinator汇报并将Barrier发送给下游的其他Operator
 * 3.其他的如TransformationOperator接收到Barrier,重复第2步,最后将Barrier发送给Sink
 * 4.Sink接收到Barrier之后重复第2步
 * 5.Coordinator接收到所有的Operator的执行ok的汇报结果,认为本次快照执行成功
 * <p>
 * 注意:
 * 1.在往介质(如HDFS)中写入快照数据的时候是异步的(为了提高效率)
 * 2.分布式快照执行时的数据一致性由Chandy-Lamport algorithm分布式快照算法保证!
 * <p>
 * 状态后端官网：https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/ops/state/state_backends/
 * <p>
 * 推荐使用的场景：常规使用状态的作业（例如分钟级窗口聚合或join）、需要开启HA的作业
 * <p>
 * checkpoint主要是用来状态恢复和容错的
 * <p>
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
public class D01_Checkpoint {
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
            设置失败重启策略：固定延迟策略：程序出现一场后，重启2次，每次延迟3秒重启；超过2次后，程序退出
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));


        DataStreamSource<String> lineDs = env.socketTextStream("linux01", 9999);
        StreamWordCountUtils.wordCount(lineDs, " ")
                .print();

        env.execute("D01_Checkpoint");
    }
}
