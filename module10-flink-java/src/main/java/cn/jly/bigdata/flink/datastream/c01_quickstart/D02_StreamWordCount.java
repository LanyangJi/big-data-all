package cn.jly.bigdata.flink.datastream.c01_quickstart;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author jilanyang
 * @date 2021/6/25 0025 15:50
 * @packageName cn.jly.bigdata.flink.datastream.c01_quickstart
 * @className D02_StreamWordCount
 */
public class D02_StreamWordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "linux01");
        int port = tool.getInt("port", 9999);

        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream(host, port)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] words = s.split(" ");
                        for (String word : words) {
                            if (StringUtils.isNotBlank(word)) {
                                collector.collect(Tuple2.of(word, 1L));
                             }
                        }
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> t) throws Exception {
                        return t.f0;
                    }
                })
                .sum(1)
                .print();

        env.execute(D02_StreamWordCount.class.getSimpleName());
    }
}
