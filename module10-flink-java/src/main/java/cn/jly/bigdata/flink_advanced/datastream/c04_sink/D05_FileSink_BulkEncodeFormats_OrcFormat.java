package cn.jly.bigdata.flink_advanced.datastream.c04_sink;

import cn.jly.bigdata.flink_advanced.datastream.beans.WordCount;
import cn.jly.bigdata.flink_advanced.utils.StreamWordCountUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.vector.Vectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * orc format
 * 为了使数据能够以 ORC 格式进行批量编码，Flink 提供了 OrcBulkWriterFactory，它采用了 Vectorizer 的具体实现。
 * 与任何其他以批量方式编码数据的列格式一样，Flink 的 OrcBulkWriter 批量写入输入元素。它使用 ORC 的 VectorizedRowBatch 来实现这一点。
 * 由于必须将输入元素转换为 VectorizedRowBatch，因此用户必须扩展抽象 Vectorizer 类并覆盖 vectorize(T element, VectorizedRowBatch batch) 方法。如您所见，该方法提供了一个 VectorizedRowBatch 实例供用户直接使用，因此用户只需编写逻辑将输入元素转换为 ColumnVectors 并将它们设置在提供的 VectorizedRowBatch 实例中。
 * <p>
 * 对于一个javaBean-WordCount，然后一个子实现来转换 WordCount 类型的元素并将它们设置在 VectorizedRowBatch 中可以
 *
 * @author jilanyang
 * @date 2021/7/26 14:36
 * @package cn.jly.bigdata.flink_advanced.datastream.c04_sink
 * @class D04_FileSink_BulkEncodeFomats_AvroFormat
 */
public class D05_FileSink_BulkEncodeFormats_OrcFormat {
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

        // 声明orc format file sink
        String schema = "struct<_col0:string,_col1:int>";
        /*
        OrcBulkWriterFactory 还可以采用 Hadoop 配置和属性，
        以便可以提供自定义 Hadoop 配置和 ORC 编写器属性。
        String schema = ...;
        Configuration conf = ...;
        Properties writerProperties = new Properties();

        writerProps.setProperty("orc.compress", "LZ4");
        // Other ORC supported properties can also be set similarly.

        final OrcBulkWriterFactory<Person> writerFactory = new OrcBulkWriterFactory<>(
            new PersonVectorizer(schema), writerProperties, conf);
         */
        final OrcBulkWriterFactory<WordCount> writerFactory = new OrcBulkWriterFactory<>(new WordCountVectorizer(schema));
        final FileSink<WordCount> sink = FileSink.forBulkFormat(
                        new Path("e:/flink/file_sink/orc_format"),
                        writerFactory
                )
                .build();

        // 执行sink
        wordCountDs.sinkTo(sink);

        env.execute("D05_FileSink_BulkEncodeFormats_OrcFormat");
    }

    /**
     * 对于一个javaBean-WordCount，
     * 然后一个子实现来转换 WordCount 类型的元素并将它们设置在 VectorizedRowBatch 中
     * <p>
     * 矢量化器
     */
    public static class WordCountVectorizer extends Vectorizer<WordCount> implements Serializable {

        public WordCountVectorizer(String schema) {
            super(schema);
        }

        @Override
        public void vectorize(WordCount wordCount, VectorizedRowBatch vectorizedRowBatch) throws IOException {
            BytesColumnVector wordColumnVector = (BytesColumnVector) vectorizedRowBatch.cols[0];
            LongColumnVector countColumnVector = (LongColumnVector) vectorizedRowBatch.cols[1];
            int row = vectorizedRowBatch.size++;
            wordColumnVector.setVal(row, wordCount.getWord().getBytes(StandardCharsets.UTF_8));
            countColumnVector.vector[row] = wordCount.getCount();
        }
    }
}
