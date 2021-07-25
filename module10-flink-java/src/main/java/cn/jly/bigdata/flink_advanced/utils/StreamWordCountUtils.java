package cn.jly.bigdata.flink_advanced.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

/**
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.utils
 * @class StreamWordCountUtils
 * @date 2021/7/25 17:19
 */
public class StreamWordCountUtils {
    /**
     * 读取一行一行的数据，并返回wordAndCount流
     *
     * @param lineDS    一行一行的数据流
     * @param delimiter 数据分隔符，默认是空格
     * @return wordAndCount
     */
    public static DataStream<Tuple2<String, Long>> wordCount(DataStream<String> lineDS, String delimiter) {
        if (StringUtils.isEmpty(delimiter)) {
            delimiter = " ";
        }

        final String finalDelimiter = delimiter;
        return lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        for (String word : s.split(finalDelimiter)) {
                            collector.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1);
    }
}
