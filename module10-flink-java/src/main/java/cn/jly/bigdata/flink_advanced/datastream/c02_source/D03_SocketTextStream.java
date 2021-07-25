package cn.jly.bigdata.flink_advanced.datastream.c02_source;

import cn.jly.bigdata.flink_advanced.utils.StreamWordCountUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c02_source
 * @class D03_SocketTextStream
 * @date 2021/7/25 17:33
 */
public class D03_SocketTextStream {
    public static void main(String[] args) throws Exception {
        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 从socket中读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("linux01", 9999);

        // word count
        StreamWordCountUtils.wordCount(socketDS, " ")
                .print();

        env.execute("D03_SocketTextStream");
    }
}
