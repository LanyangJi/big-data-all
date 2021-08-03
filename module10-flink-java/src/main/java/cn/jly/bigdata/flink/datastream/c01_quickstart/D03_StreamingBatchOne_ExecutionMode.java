package cn.jly.bigdata.flink.datastream.c01_quickstart;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 从flink 1.12开始，真正实现了流批统一处理。
 * 以往的dataSet已经不推荐使用了
 * <p>
 * DataStream API既可以执行流式处理，也可以执行批处理
 * 只要通过指定execution mode（执行模式）即可
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink.datastream.c01_quickstart
 * @class D03_StreamingBatchOne_ExecutionMode
 * @date 2021/7/24 16:56
 */
public class D03_StreamingBatchOne_ExecutionMode {
    public static void main(String[] args) throws Exception {

        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 执行执行模式 -> 批处理 or 流处理
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // flink会自动根据数据源（无界数据或者有界数据）选择流还是批处理模式；正式开发中也是推荐这种方式
        // 默认值是按照流来处理
        // env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 读取数据
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");

        // 切割单词
        SingleOutputStreamOperator<String> wordDS = lineDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        // 转换为 （word, 1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = wordDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

        // 分组
        KeyedStream<Tuple2<String, Integer>, String> keyedDS = wordAndOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> t) throws Exception {
                return t.f0;
            }
        });

        // 统计聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCountDS = keyedDS.sum(1);

        // 打印
        wordAndCountDS.print();

        env.execute();
    }
}
