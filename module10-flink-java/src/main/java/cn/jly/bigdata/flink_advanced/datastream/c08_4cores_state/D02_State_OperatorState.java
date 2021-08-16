package cn.jly.bigdata.flink_advanced.datastream.c08_4cores_state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;

/**
 * 使用ListState存储offset模拟kafka的offset
 *
 * 且用到了故障重启时借助checkpoint进行状态恢复
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c08_4cores_state
 * @class D02_State_OperatorState
 * @date 2021/8/1 16:20
 */
public class D02_State_OperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1); // 并行度设为1方便观察
        // 设置checkpoint时间间隔和磁盘路径以及代码遇到异常后的重启策略
        env.enableCheckpointing(1000); // 每隔1s执行一次checkpoint，一般设置为秒级，毫秒级（比如十几毫秒）比较影响性能，毕竟有的状态比较大
        // 旧的 FsStateBackend 相当于使用 HashMapStateBackend 和 FileSystemCheckpointStorage。
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/JLY/test/ckp");
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 固定延迟策略：程序出现一场后，重启2次，每次延迟3秒重启；超过2次后，程序推出
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));

        env.addSource(new MyKafkaAdvanceSource())
                .print();

        env.execute("D02_State_OperatorState");
    }

    // 使用ListState存储offset模拟kafka的offset
    public static class MyKafkaAdvanceSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
        // 存储偏移量，多个并行度可以把值类型换成 ListState<Map<分区id, Long>> = ...
        // 用来存放offset
        private ListState<Long> offsetState;
        // 用来存放offset的值
        private Long offset = 0L;
        private boolean flag = true;

        /**
         * 定时执行，将state状态从内存存入checkpoint中
         * env.enableCheckpointing(1000); // 每隔1s执行一次checkpoint
         * env.getCheckpointConfig().setCheckpointStorage("file:///d:/JLY/test/ckp");
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            offsetState.clear(); // 清理内存数据并存入checkpoint中
            offsetState.add(offset);
        }

        /**
         * 状态初始化
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 状态描述器
            ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<>("offset_state", Long.class);
            // 初始化operator state
            offsetState = context.getOperatorStateStore().getListState(stateDescriptor);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (flag) {
                Iterator<Long> iterator = offsetState.get().iterator();
                if (iterator.hasNext()) {
                    offset = iterator.next();
                }
                offset += 1;
                // 任务id
                int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
                ctx.collect("subTaskId: " + subTaskIndex + ", 当前的offset值为：" + offset);
                Thread.sleep(1000);

                // 模拟异常
                if (offset % 5 == 0) {
                    throw new RuntimeException("bug出现了...");
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
