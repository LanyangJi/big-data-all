package cn.jly.bigdata.flink_advanced.datastream.c03_transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 重分区
 * 类似spark中的repartition，但是功能更强大，可以直接解决数据倾斜问题
 * <p>
 * Flink也有数据倾斜的时候，比如当前有数据量大概10亿条数据需要处理，在处理过程中可能会发生如图所示的状况，出现了数据倾斜，
 * 其他3台机器执行完毕也要等待机器1执行完毕后才算整体将任务完成；
 * <p>
 * source（10亿条） -> 分区1（9.9999..亿条） - 两小时处理
 * |
 * -----------------> 分区2（几条） - 0.2秒
 * |
 * -----------------> 分区3（几条） - 0.2秒
 * |
 * -----------------> 分区4（几条） - 0.2秒
 * <p>
 * 所以在实际的工作中，出现这种情况比较好的解决方案就是rebalance(内部使用round robin方法将数据均匀打散)
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c03_transformation
 * @class D04_Rebalance
 * @date 2021/7/25 22:14
 */
public class D04_Rebalance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Long> sequenceDS = env.fromSequence(8, 100);
        SingleOutputStreamOperator<Long> filterDS = sequenceDS.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 10;
            }
        });

        // rebalance之前可能会数据倾斜
        filterDS.map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Long value) throws Exception {
                        // 子任务编号/分区编号
                        int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
                        return Tuple2.of(subTaskId, 1);
                    }
                })
                // 按照分区分组
                .keyBy(t -> t.f0)
                // 按照子任务id/分区编号分组，并统计元素个数
                .sum(1)
                .print();

        // rebalance解决数据倾斜
        filterDS.rebalance().map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Long value) throws Exception {
                        // 子任务编号/分区编号
                        int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
                        return Tuple2.of(subTaskId, 1);
                    }
                })
                // 按照分区分组
                .keyBy(t -> t.f0)
                // 按照子任务id/分区编号分组，并统计元素个数
                .sum(1)
                .printToErr();

        env.execute("D04_Rebalance");
    }
}
