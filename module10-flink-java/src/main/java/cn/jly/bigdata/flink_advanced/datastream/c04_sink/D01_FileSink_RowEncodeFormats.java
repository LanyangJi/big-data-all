package cn.jly.bigdata.flink_advanced.datastream.c04_sink;


import cn.jly.bigdata.flink_advanced.utils.StreamWordCountUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * todo 完善 flink的 file sink
 *
 * @author jilanyang
 * @date 2021/7/26 10:11
 * @package cn.jly.bigdata.flink_advanced.datastream.c04_sink
 * @class D01_FileSink_RowEncodeFormats
 */
public class D01_FileSink_RowEncodeFormats {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // file source
        DataStreamSource<String> fileDs = env.readTextFile("input/word.txt");
        // wordcount
        DataStream<Tuple2<String, Long>> wordAndCountDs = StreamWordCountUtils.wordCount(fileDs, " ");

        // 声明file sink
        /*
        此示例创建一个简单的接收器，将记录分配给默认的一小时时间段。它还指定了一个滚动策略，
        该策略在以下 3 个条件中的任何一个条件下滚动进行中的part文件：
            - 它包含至少 15 分钟的数据
            - 最近 5 分钟没有收到新记录
            - 文件大小已达到 1 GB（写入最后一条记录后）
         */
        FileSink<Tuple2<String, Long>> fileSink = FileSink.forRowFormat(
                        new Path("e:/flink/file_sink/wc"),
                        new SimpleStringEncoder<Tuple2<String, Long>>()
                )
                // 配置滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                // 翻转间隔15分钟
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                // 不活动间隔5分钟
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                // part file最大大小
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build()
                )
                .build();

        // 执行file sink
        wordAndCountDs.sinkTo(fileSink);

        env.execute("D01_FileSink_RowEncodeFormats");
    }
}
