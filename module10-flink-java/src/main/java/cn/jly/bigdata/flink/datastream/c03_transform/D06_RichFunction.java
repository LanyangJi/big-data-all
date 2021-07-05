package cn.jly.bigdata.flink.datastream.c03_transform;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * “富函数”是 DataStream API 提供的一个函数类的接口，所有 Flink 函数类都
 * 有其 Rich 版本。它与常规函数的不同在于，可以获取运行环境的上下文，并拥有一
 * 些生命周期方法，所以可以实现更复杂的功能。
 * ⚫ RichMapFunction
 * ⚫ RichFlatMapFunction
 * ⚫ RichFilterFunction
 * ⚫ …
 * Rich Function 有一个生命周期的概念。典型的生命周期方法有：
 * ⚫ open()方法是 rich function 的初始化方法，当一个算子例如 map 或者 filter被调用之前 open()会被调用。
 * ⚫ close()方法是生命周期中的最后一个调用的方法，做一些清理工作。
 * ⚫ getRuntimeContext()方法提供了函数的 RuntimeContext 的一些信息，例如函
 * 数执行的并行度，任务的名字，以及 state 状态
 *
 * @author jilanyang
 * @date 2021/7/2 9:58
 * @packageName cn.jly.bigdata.flink.datastream.c03_transform
 * @className D06_RichFunction
 */
public class D06_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.readTextFile("input/word.txt")
                .flatMap(new MyRichFlatMapFunction())
                .print();

        env.execute("D06_RichFunction");
    }

    /**
     * 自定义富函数
     */
    public static class MyRichFlatMapFunction extends RichFlatMapFunction<String, Tuple2<String, String>> {
        String taskName;

        @Override
        public void open(Configuration parameters) throws Exception {
            RuntimeContext runtimeContext = getRuntimeContext();
            taskName = runtimeContext.getTaskName();
        }

        @Override
        public void close() throws Exception {
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(taskName, word));
            }
        }
    }
}
