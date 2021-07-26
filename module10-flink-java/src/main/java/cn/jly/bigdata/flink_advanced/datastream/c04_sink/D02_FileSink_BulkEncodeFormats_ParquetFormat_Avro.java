package cn.jly.bigdata.flink_advanced.datastream.c04_sink;


import cn.jly.bigdata.flink_advanced.datastream.beans.WordCount;
import cn.jly.bigdata.flink_advanced.utils.StreamWordCountUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 批量编码接收器的创建类似于行编码接收器，但我们必须指定 BulkWriter.Factory 而不是指定编码器。
 * BulkWriter 逻辑定义了如何添加和刷新新元素，以及如何最终确定一批记录以进行进一步编码。
 * Flink 内置了四个 BulkWriter 工厂：
 * - ParquetWriterFactory
 * - AvroWriterFactory
 * - SequenceFileWriterFactory
 * - CompressWriterFactory
 * - OrcBulkWriterFactory
 * <p>
 * 批量格式只能具有扩展 CheckpointRollingPolicy 的滚动策略。后者在每个检查点滚动。策略可以根据大小或处理时间额外滚动。
 * <p>
 * AVRO序列化
 * Avro是一个数据序列化系统，设计用于支持大批量数据交换的应用。
 * 它的主要特点有：支持二进制序列化方式，可以便捷，快速地处理大量数据；动态语言友好，
 * Avro提供的机制使动态语言可以方便地处理Avro数据。
 * <p>
 * 当前市场上有很多类似的序列化系统，如Google的Protocol Buffers, Facebook的Thrift。
 * 这些系统反响良好，完全可以满足普通应用的需求。
 * 针对重复开发的疑惑，Doug Cutting撰文解释道：Hadoop现存的RPC系统遇到一些问题，
 * 如性能瓶颈(当前采用IPC系统，它使用Java自带的DataOutputStream和DataInputStream)；
 * 需要服务器端和客户端必须运行相同版本的Hadoop；只能使用Java开发等。
 * 但现存的这些序列化系统自身也有毛病，以Protocol Buffers为例，它需要用户先定义数据结构，
 * 然后根据这个数据结构生成代码，再组装数据。如果需要操作多个数据源的数据集，
 * 那么需要定义多套数据结构并重复执行多次上面的流程，这样就不能对任意数据集做统一处理。
 * 其次，对于Hadoop中Hive和Pig这样的脚本系统来说，使用代码生成是不合理的。
 * 并且Protocol Buffers在序列化时考虑到数据定义与数据可能不完全匹配，在数据中添加注解，
 * 这会让数据变得庞大并拖慢处理速度。其它序列化系统有如Protocol Buffers类似的问题。
 * 所以为了Hadoop的前途考虑，Doug Cutting主导开发一套全新的序列化系统，这就是Avro，于09年加入Hadoop项目族中。
 * 上面通过与Protocol Buffers的对比，大致清楚了Avro的特长。下面着重关注Avro的细节部分。
 * <p>
 * Avro依赖模式(Schema)来实现数据结构定义。可以把模式理解为Java的类，它定义每个实例的结构，可以包含哪些属性。可以根据类来产生任意多个实例对象。对实例序列化操作时必须需要知道它的基本结构，也就需要参考类的信息。这里，根据模式产生的Avro对象类似于类的实例对象。每次序列化/反序列化时都需要知道模式的具体结构。所以，在Avro可用的一些场景下，如文件存储或是网络通信，都需要模式与数据同时存在。Avro数据以模式来读和写(文件或是网络)，并且写入的数据都不需要加入其它标识，这样序列化时速度快且结果内容少。由于程序可以直接根据模式来处理数据，所以Avro更适合于脚本语言的发挥。
 * Avro的模式主要由JSON对象来表示，它可能会有一些特定的属性，用来描述某种类型(Type)的不同形式。Avro支持八种基本类型(Primitive Type)和六种混合类型(Complex Type)。基本类型可以由JSON字符串来表示。每种不同的混合类型有不同的属性(Attribute)来定义，有些属性是必须的，有些是可选的，如果需要的话，可以用JSON数组来存放多个JSON对象定义。在这几种Avro定义的类型的支持下，可以由用户来创造出丰富的数据结构来，支持用户纷繁复杂的数据。
 * Avro支持两种序列化编码方式：二进制编码和JSON编码。使用二进制编码会高效序列化，并且序列化后得到的结果会比较小；而JSON一般用于调试系统或是基于WEB的应用。对Avro数据序列化/反序列化时都需要对模式以深度优先(Depth-First)，从左到右(Left-to-Right)的遍历顺序来执行。基本类型的序列化容易解决，混合类型的序列化会有很多不同规则。对于基本类型和混合类型的二进制编码在文档中规定，按照模式的解析顺序依次排列字节。对于JSON编码，联合类型(Union Type)就与其它混合类型表现不一致。 Avro为了便于MapReduce的处理定义了一种容器文件格式(Container File Format)。这样的文件中只能有一种模式，所有需要存入这个文件的对象都需要按照这种模式以二进制编码的形式写入。对象在文件中以块(Block)来组织，并且这些对象都是可以被压缩的。块和块之间会存在同步标记符(Synchronization Marker)，以便MapReduce方便地切割文件用于处理。
 * <p>
 * 本例子：
 * 示范将avro数据写入parquet文件格式
 *
 * @author jilanyang
 * @date 2021/7/26 10:11
 * @package cn.jly.bigdata.flink_advanced.datastream.c04_sink
 * @class D02_FileSink_BulkEncodeFormats_ParquetFormat_Avro
 */
public class D02_FileSink_BulkEncodeFormats_ParquetFormat_Avro {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // file source
        DataStreamSource<String> fileDs = env.readTextFile("input/word.txt");
        // wordcount
        SingleOutputStreamOperator<WordCount> wordAndCountDs = StreamWordCountUtils.wordCount(fileDs, " ")
                .map(new MapFunction<Tuple2<String, Long>, WordCount>() {
                    @Override
                    public WordCount map(Tuple2<String, Long> value) throws Exception {
                        return new WordCount(value.f0, value.f1);
                    }
                });

        // 声明file sink 之parquetAvroSink
        // 将 Avro 数据写入 Parquet 格式的 FileSink 可以这样创建：
        //Schema schema = Schema.createRecord(
        //        "WordCount",
        //        "单词统计类",
        //        "cn.jly.bigdata.flink_advanced.datastream.c04_sink",
        //        false,
        //        Arrays.asList(
        //                new Schema.Field("word", Schema.create(Schema.Type.STRING)),
        //                new Schema.Field("count", Schema.create(Schema.Type.LONG))
        //        )
        //);
        FileSink<WordCount> parquetAvroSink = FileSink.forBulkFormat(
                new Path("e:/flink/file_sink/parquet_avro"),
                //ParquetAvroWriters.forGenericRecord(schema)
                ParquetAvroWriters.forReflectRecord(WordCount.class)
        ).build();

        // 执行file sink 之parquetAvroSink
        wordAndCountDs.sinkTo(parquetAvroSink);

        env.execute("D02_FileSink_BulkEncodeFormats_ParquetFormat_Avro");
    }

}
