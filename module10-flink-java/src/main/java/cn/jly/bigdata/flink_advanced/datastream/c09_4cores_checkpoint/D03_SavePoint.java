package cn.jly.bigdata.flink_advanced.datastream.c09_4cores_checkpoint;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * SavePoint保存点  —————— 简单理解，savePoint就是手动的checkpoint
 * 保存点,类似于以前玩游戏的时候,遇到难关了/遇到boss了,赶紧手动存个档,然后接着玩,如果失败了,赶紧从上次的存档中恢复,然后接着玩
 * <p>
 * 在实际开发中,可能会遇到这样的情况:
 * 如要对集群进行停机维护/扩容...那么这时候需要执行一次Savepoint也就是执行一次手动的Checkpoint/也就是手动的发一个barrier栅栏,
 * 那么这样的话,程序的所有状态都会被执行快照并保存, 当维护/扩容完毕之后,可以从上一次Savepoint的目录中进行恢复!
 *
 * 文档地址：
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/ops/state/savepoints/
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c09_4cores_checkpoint
 * @class D03_SavePoint
 * @date 2021/8/1 22:34
 */
public class D03_SavePoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.execute("D03_SavePoint");
    }
}
