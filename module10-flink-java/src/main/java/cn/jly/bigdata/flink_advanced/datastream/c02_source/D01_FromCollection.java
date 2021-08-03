package cn.jly.bigdata.flink_advanced.datastream.c02_source;

import cn.jly.bigdata.flink_advanced.utils.StreamWordCountUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从本地集合创建source
 * <p>
 * 1. 基于文件
 * 2. 基于socket
 * 3. 基于本地集合
 * 4. 自定义source(kafka等)
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c02_source
 * @class D01_FromCollection
 * @date 2021/7/25 17:10
 */
public class D01_FromCollection {
    public static void main(String[] args) throws Exception {
        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 从本地集合读取数据
        // DataStreamSource<String> lineDS = env.fromCollection(Arrays.asList("tom spark flink spark", "tom flink scala flink"));
        DataStreamSource<String> lineDS = env.fromElements("tom spark flink spark", "tom flink scala flink");

        // 数据处理
        StreamWordCountUtils.wordCount(lineDS, " ")
                .print();

        env.execute("D01_FromCollection");
    }
}
