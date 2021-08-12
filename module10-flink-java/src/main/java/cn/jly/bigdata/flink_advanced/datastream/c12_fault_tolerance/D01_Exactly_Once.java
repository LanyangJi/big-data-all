package cn.jly.bigdata.flink_advanced.datastream.c12_fault_tolerance;

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
 *
 * checkpoint说明：
 * 1. 按照上面我们介绍的机制，每次在把快照存储到我们的状态后端时，如果是同步进行就会阻塞正常任务，从而引入延迟。
 * 因此 Flink 在做快照存储时，可采用异步方式。
 * 2. 此外，由于 checkpoint 是一个全局状态，用户保存的状态可能非常大，多数达 G 或者 T 级别。
 * 在这种情况下，checkpoint 的创建会非常慢，而且执行时占用的资源也比较多，因此 Flink 提出了增量快照的概念。
 * 也就是说，每次都是进行的全量 checkpoint，是基于上次进行更新的。
 *
 * flink两种事务写入的说明：
 * 这两种方式区别主要在于：
 * 1. WAL方式通用性更强，适合几乎所有外部系统，但也不能提供百分百端到端的Exactly-Once，因为WAL预习日志会先写内存，而内存是易失介质。
 * 2. 如果外部系统自身就支持事务（比如MySQL、Kafka），可以使用2PC方式，可以提供百分百端到端的Exactly-Once。
 * 所以，事务写的方式能提供端到端的Exactly-Once一致性，它的代价也是非常明显的，就是牺牲了延迟。输出数据不再是实时写入到外部系统，而是分批次地提交。目前来说，没有完美的故障恢复和Exactly-Once保障机制，对于开发者来说，需要在不同需求之间权衡。
 *
 * 注意：精确一次? 有效一次!
 * 1. 有些人可能认为『精确一次』描述了事件处理的保证，其中流中的每个事件只被处理一次。
 * 实际上，没有引擎能够保证正好只处理一次。在面对任意故障时，不可能保证每个算子中的用户定义逻辑在每个事件中只执行一次，
 * 因为用户代码被部分执行的可能性是永远存在的。
 * 2. 那么，当引擎声明『精确一次』处理语义时，它们能保证什么呢？如果不能保证用户逻辑只执行一次，那么什么逻辑只执行一次？
 * 当引擎声明『精确一次』处理语义时，它们实际上是在说，它们可以保证引擎管理的状态更新只提交一次到持久的后端存储。
 * 事件的处理可以发生多次，但是该处理的效果只在持久后端状态存储中反映一次。
 * 因此，我们认为有效地描述这些处理语义最好的术语是『有效一次』（effectively once）
 *
 * @author jilanyang
 * @date 2021/8/9 21:43
 * @package cn.jly.bigdata.flink_advanced.datastream.c12_fault_tolerance
 * @class D01_Exactly_Once
 */
public class D01_Exactly_Once {
    public static void main(String[] args) {

    }
}
