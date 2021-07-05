package cn.jly.bigdata.flink.datastream.c01_quickstart;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author jilanyang
 * @date 2021/6/25 0025 15:41
 * @packageName cn.jly.bigdata.flink.datastream.c01_quickstart
 * @className D01_BatchWordCount
 */
public class D01_BatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.readTextFile("input/word.txt")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words) {
                            if (StringUtils.isNotEmpty(word)) {
                                collector.collect(Tuple2.of(word, 1L));
                            }
                        }
                    }
                })
                .groupBy(0)
                .sum(1)
                .print();

        /*
        异常的原因就是说，自上次执行以来，没有定义新的数据接收器。
        对于离线批处理的算子，如：“count()”、“collect()”或“print()”等既有sink功能，还有触发的功能。
        我们上面调用了print()方法，会自动触发execute，所以最后面的一行执行器没有数据可以执行。
         */
        // env.execute();
    }
}
