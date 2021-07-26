package cn.jly.bigdata.flink_advanced.datastream.c04_sink;

import cn.jly.bigdata.flink_advanced.datastream.beans.WordCount;
import cn.jly.bigdata.flink_advanced.utils.StreamWordCountUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroFormatFactory;
import org.apache.flink.formats.avro.AvroWriters;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Avro 格式
 * Flink 还提供了将数据写入 Avro 文件的内置支持。
 * 可以在 AvroWriters 类中找到创建 Avro 编写器工厂及其相关文档的便捷方法列表。
 * 要在您的应用程序中使用 Avro 编写器，您需要添加以下依赖项：
 * <dependency>
 * <groupId>org.apache.flink</groupId>
 * <artifactId>flink-avro_2.12</artifactId>
 * <version>1.13.0</version>
 * </dependency>
 *
 * @author jilanyang
 * @date 2021/7/26 14:36
 * @package cn.jly.bigdata.flink_advanced.datastream.c04_sink
 * @class D04_FileSink_BulkEncodeFomats_AvroFormat
 */
public class D04_FileSink_BulkEncodeFormats_AvroFormat {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // file source
        DataStreamSource<String> fileDs = env.readTextFile("input/word.txt");

        SingleOutputStreamOperator<WordCount> wordCountDs = StreamWordCountUtils.wordCount(fileDs, "")
                .map(new MapFunction<Tuple2<String, Long>, WordCount>() {
                    @Override
                    public WordCount map(Tuple2<String, Long> value) throws Exception {
                        return new WordCount(value.f0, value.f1);
                    }
                });

        // 声明avro format file sink
        FileSink<WordCount> fileSink = FileSink.forBulkFormat(
                new Path("e:/flink/file_sink/avro_format"),
                AvroWriters.forReflectRecord(WordCount.class)
        ).build();

        // 执行sink
        wordCountDs.sinkTo(fileSink);

        env.execute("D04_FileSink_BulkEncodeFormats_AvroFormat");
    }
}
