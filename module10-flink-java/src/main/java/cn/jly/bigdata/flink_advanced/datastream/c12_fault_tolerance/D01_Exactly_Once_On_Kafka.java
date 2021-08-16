package cn.jly.bigdata.flink_advanced.datastream.c12_fault_tolerance;


import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * 流处理引擎通常为应用程序提供了三种数据处理语义：
 * At most noce最多一次(有可能会有数据丢失)
 * At least once至少一次(可能导致数据重复计算)
 * Exactly once精确一次(是指数据只被正确处理一次，但不一定只会被计算一次，因为回退到检查点的时候，再从检查点的位置保存的偏移量重新计算，
 * 可能先前被计算过的数据会被再次计算，所以这边精确一次就是指恰好正确处理一次)
 * <p>
 * 如下是对这些不同处理语义的宽松定义(一致性由弱到强)：
 * At most noce < At least once < Exactly once < End to End Exactly once
 * flink搭配kafka就能够实现端到端的精准一致性，从source -> transformation -> sink都能保证exactly once
 * source: 可以重置偏移量
 * transformation: checkpoint（分布式快照,且是异步方式）
 * sink: 幂等写入、事务写入（实现方式有预写日志、两阶段提交）
 * <p>
 * checkpoint说明：
 * 1. 按照上面我们介绍的机制，每次在把快照存储到我们的状态后端时，如果是同步进行就会阻塞正常任务，从而引入延迟。
 * 因此 Flink 在做快照存储时，可采用异步方式。
 * 2. 此外，由于 checkpoint 是一个全局状态，用户保存的状态可能非常大，多数达 G 或者 T 级别。
 * 在这种情况下，checkpoint 的创建会非常慢，而且执行时占用的资源也比较多，因此 Flink 提出了增量快照的概念。
 * 也就是说，每次都是进行的全量 checkpoint，是基于上次进行更新的。
 * <p>
 * flink两种事务写入的说明：
 * 这两种方式区别主要在于：
 * 1. WAL方式通用性更强，适合几乎所有外部系统，但也不能提供百分百端到端的Exactly-Once，因为WAL预习日志会先写内存，而内存是易失介质。
 * 2. 如果外部系统自身就支持事务（比如MySQL、Kafka），可以使用2PC方式，可以提供百分百端到端的Exactly-Once。
 * 所以，事务写的方式能提供端到端的Exactly-Once一致性，它的代价也是非常明显的，就是牺牲了延迟。输出数据不再是实时写入到外部系统，而是分批次地提交。目前来说，没有完美的故障恢复和Exactly-Once保障机制，对于开发者来说，需要在不同需求之间权衡。
 * <p>
 * 注意：精确一次? 有效一次!
 * 1. 有些人可能认为『精确一次』描述了事件处理的保证，其中流中的每个事件只被处理一次。
 * 实际上，没有引擎能够保证正好只处理一次。在面对任意故障时，不可能保证每个算子中的用户定义逻辑在每个事件中只执行一次，
 * 因为用户代码被部分执行的可能性是永远存在的。
 * 2. 那么，当引擎声明『精确一次』处理语义时，它们能保证什么呢？如果不能保证用户逻辑只执行一次，那么什么逻辑只执行一次？
 * 当引擎声明『精确一次』处理语义时，它们实际上是在说，它们可以保证引擎管理的状态更新只提交一次到持久的后端存储。
 * 事件的处理可以发生多次，但是该处理的效果只在持久后端状态存储中反映一次。
 * 因此，我们认为有效地描述这些处理语义最好的术语是『有效一次』（effectively once）
 * <p>
 * 本文示例，flink整合kafka实现end to end exactly once(端到端的精确一致性)
 * 流程：
 * kafkaTopic -> flink source -> flink transformation -> flink sink -> kafkaTopic2
 *
 * @author jilanyang
 * @date 2021/8/9 21:43
 * @package cn.jly.bigdata.flink_advanced.datastream.c12_fault_tolerance
 * @class D01_Exactly_Once_On_Kafka
 */
public class D01_Exactly_Once_On_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // TODO checkpoint一些常用的配置
        //===========类型1:必须参数=============
        // 每隔1s执行一次checkpoint，一般设置为秒级或者分钟，毫秒级（比如十几毫秒）比较影响性能，毕竟有的状态比较大
        env.enableCheckpointing(1000);
        // 方式一：旧的 FsStateBackend 相当于使用 HashMapStateBackend 和 FileSystemCheckpointStorage。
        env.setStateBackend(new HashMapStateBackend());
        if (SystemUtils.IS_OS_WINDOWS) { // 根据运行程序的操作系统类型设置checkpoint存储目录
            env.getCheckpointConfig().setCheckpointStorage("file:///d:/JLY/test/ckp");
        } else {
            env.getCheckpointConfig().setCheckpointStorage("hdfs://linux01:8020/flink-checkpoint/checkpoing");
        }
        // 方式二：使用rocksDB作为状态后端
        // env.setStateBackend(new EmbeddedRocksDBStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs://linux01:8020/flink-checkpoint/checkpoing");

        //===========类型2:建议参数===========
        //设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms
        // (为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了)
        //如:高速公路上,每隔1s关口放行一辆车,但是规定了两车之前的最小车距为500m
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);//默认是0
        //设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是  false不是
        // env.getCheckpointConfig().setFailOnCheckpointingErrors(false);//默认是true，该api过期了，推荐使用下面的次数约定
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);//默认值为0，表示不容忍任何检查点失败
        //设置是否清理检查点,即在 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true,当作业被取消时，删除外部的checkpoint(默认值)
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false,当作业被取消时，保留外部的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //===========类型3:直接使用默认的即可===============
        //设置checkpoint的执行模式为EXACTLY_ONCE(默认)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
        env.getCheckpointConfig().setCheckpointTimeout(60000);//默认10分钟
        //设置同一时间有多少个checkpoint可以同时执行; 这边如果设置大于1的话是和配置项setMinPauseBetweenCheckpoints冲突的，二者取其一
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);//默认为1

        /*
            自动重启策略与恢复：
            1. 默认重启策略
            2. 无重启策略
            3. 固定延迟重启策略——开发中使用
            4. 失败率重启策略——开发偶尔使用
            设置失败重启策略：固定延迟策略：程序出现异常后，重启2次，每次延迟3秒重启；超过2次后，程序退出
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));

        String brokers = "linux01:9092";

        // flink kafka source - 新的方式
//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                .setBootstrapServers(brokers)
//                .setTopics("test")
//                .setGroupId("lanyangji")
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        // kafka source - 传统的方式
        Properties sourceProperties = new Properties();
        // kafka集群地址
        sourceProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        // 消费者id
        sourceProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "lanyangji");
        // 有offset记录从记录位置开始消费，没有则从latest开始消费；earliest有offset记录则从记录位置开始消费，没有则从earliest位置开始消费
        sourceProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 会开启一个后台进程，每隔5秒检测一下分区情况，实现动态分区检测
        sourceProperties.setProperty("flink.partition-discovery.interval-millis", "5000");
        // 自动提交（提交到默认主题，随着checkpoint存储在checkpoint（为了容错）和默认主题（为了方便外部工具获取）中）
        sourceProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 自动提交的时间间隔
        sourceProperties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000");

        // FlinkKafkaConsumer里面实现了CheckpointedFunction,里面实现了offset的维护
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                "test",
                new SimpleStringSchema(),
                sourceProperties
        );
        // 在做checkpoint的时候提交offset(为了容错)，默认就是true
        kafkaSource.setCommitOffsetsOnCheckpoints(true);

        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // transformation
        SingleOutputStreamOperator<String> resDS = kafkaDS.flatMap(
                        new FlatMapFunction<String, WordAndCount>() {
                            @Override
                            public void flatMap(String value, Collector<WordAndCount> out) throws Exception {
                                String[] words = value.split(",");
                                for (String word : words) {
                                    out.collect(new WordAndCount(word, 1L));
                                }
                            }
                        }
                )
                .keyBy(WordAndCount::getWord)
                .reduce(
                        new ReduceFunction<WordAndCount>() {
                            @Override
                            public WordAndCount reduce(WordAndCount wc1, WordAndCount wc2) throws Exception {
                                wc1.setCount(wc1.getCount() + wc2.getCount());
                                return wc1;
                            }
                        }
                )
                .map(
                        new MapFunction<WordAndCount, String>() {
                            @Override
                            public String map(WordAndCount value) throws Exception {
                                return JSON.toJSONString(value);
                            }
                        }
                );


        // flink kafka sink - 后期项目中使用自定义的序列化规则
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "5000"); // 设置事务的超时时间
        // 实现了TwoPhaseCommitSinkFunction（两阶段提交），两阶段提交又实现了CheckpointedFunction
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "my-topic",                  // target topic
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),    // serialization schema
                properties,                  // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        ); // fault-tolerance
        resDS.addSink(kafkaProducer);

        env.execute("D01_Exactly_Once_On_Kafka");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WordAndCount {
        private String word;
        private Long count;
    }
}
