package cn.jly.bigdata.flink_advanced.datastream.c03_transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 其他涉及到分区的api
 * src/main/resources/flink其他分区api.png 图片中有一些说明
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c03_transformation
 * @class D05_OtherPartition
 * @date 2021/7/25 23:28
 */
public class D05_OtherPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 读取数据
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");
        // 切割单词
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        // 数据全部发往第一个subTask
        DataStream<Tuple2<String, Integer>> globalDS = wordAndOneDS.global();
        // 广播
        DataStream<Tuple2<String, Integer>> broadcastDS = wordAndOneDS.broadcast();
        // 上下游并发度一样时，一对一发送
        DataStream<Tuple2<String, Integer>> forwardDS = wordAndOneDS.forward();
        // 随机均匀分配
        DataStream<Tuple2<String, Integer>> shuffleDS = wordAndOneDS.shuffle();
        // 轮循均匀分配
        DataStream<Tuple2<String, Integer>> rebalanceDS = wordAndOneDS.rebalance();
        // 本地轮流分配：比如上游4个分区，下游2个分区，则前两个发到下游第一个，后俩发到下游第二个
        DataStream<Tuple2<String, Integer>> rescaleDS = wordAndOneDS.rescale();
        // 自定义分区
        DataStream<Tuple2<String, Integer>> partitionCustomDS = wordAndOneDS.partitionCustom(
                new Partitioner<String>() {
                    /**
                     * 根据key返回分区编号
                     *
                     * @param key
                     * @param i
                     * @return
                     */
                    @Override
                    public int partition(String key, int i) {
                        return 0;
                    }
                },
                new KeySelector<Tuple2<String, Integer>, String>() {
                    /**
                     * 指定key
                     *
                     * @param value
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });

        globalDS.print("globalDS");
        broadcastDS.print("broadcastDS");
        forwardDS.print("forwardDS");
        shuffleDS.print("shuffleDS");
        rebalanceDS.print("rebalanceDS");
        rescaleDS.print("rescaleDS");
        partitionCustomDS.print("partitionCustomDS");

        env.execute("D05_OtherPartition");
    }
}
