package cn.jly.bigdata.flink_advanced.datastream.c15_accumulator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 累加器是带有加法运算和最终累加结果的简单结构，可在作业结束后使用。
 * 最直接的累加器是计数器：您可以使用 Accumulator.add(V value) 方法递增它。在作业结束时，Flink 会将所有部分结果汇总（合并）并将结果发送给客户端。
 * 累加器在调试期间或如果您想快速了解有关数据的更多信息时很有用。
 * <p>
 * Flink 目前内置了以下累加器。它们中的每一个都实现了 Accumulator 接口。
 * IntCounter 、 LongCounter 和 DoubleCounter ：有关使用计数器的示例，请参见下文。
 * Histogram(直方图)：离散数量的 bin 的直方图实现。在内部，它只是一个从整数到整数的映射。您可以使用它来计算值的分布，例如字数统计程序的每行字数分布。
 * <p>
 * 所有累加器为每个作业共享一个命名空间。因此，您可以在作业的不同操作员功能中使用相同的累加器。 Flink 会在内部合并所有同名的累加器。
 * 关于累加器和迭代的说明：目前累加器的结果只有在整个工作结束后才可用。我们还计划在下一次迭代中提供上一次迭代的结果。
 * 您可以使用聚合器来计算每次迭代的统计数据，并根据此类统计数据确定迭代的终止。
 *
 * @author jilanyang
 * @date 2021/9/6 13:48
 */
public class D01_Accumulator_Counter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        env.readTextFile("input/word.txt")
                .flatMap(
                        new RichFlatMapFunction<String, Tuple2<String, Long>>() {
                            // 定义一个Long类型的计数器，统计所有单词的数量
                            private LongCounter wordCounter = new LongCounter();

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                getRuntimeContext().addAccumulator("word-counter", this.wordCounter);
                            }

                            @Override
                            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                                String[] words = line.split(" ");
                                for (String word : words) {
                                    // count累加
                                    this.wordCounter.add(1);

                                    collector.collect(Tuple2.of(word, 1L));
                                }
                            }
                        }
                )
                .keyBy(
                        t -> t.f0
                )
                .sum(1)
                .print();

        JobExecutionResult executionResult = env.execute("D01_Accumulator_Counter");
        long counter = executionResult.getAccumulatorResult("word-counter");
        System.out.println("counter = " + counter);
    }
}
