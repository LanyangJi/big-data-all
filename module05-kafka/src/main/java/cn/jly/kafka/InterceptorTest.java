package cn.jly.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.message.ApiVersionsResponseData;

import java.util.*;

/**
 * Producer 拦截器(interceptor)是在 Kafka 0.10 版本被引入的，主要用于实现 clients 端的定
 * 制化控制逻辑。
 * 对于 producer 而言，interceptor 使得用户在消息发送前以及 producer 回调逻辑前有机会
 * 对消息做一些定制化需求，比如修改消息等。同时，producer 允许用户指定多个 interceptor
 * 按序作用于同一条消息从而形成一个拦截链(interceptor chain)。Intercetpor 的实现接口是
 * org.apache.kafka.clients.producer.ProducerInterceptor
 * <p>
 * 实现一个简单的双 interceptor 组成的拦截链。第一个 interceptor 会在消息发送前将时间
 * 戳信息加到消息 value 的最前部；第二个 interceptor 会在消息发送后更新成功发送消息数或
 * 失败发送消息数。
 *
 * @author Administrator
 * @date 2021/5/27 0027 13:33
 * @packageName cn.jly.kafka
 * @className InterceptorTest
 */
public class InterceptorTest {

    public static void main(String[] args) {
        final Properties properties = KafkaUtils.PRODUCER_CONFIG_PROPERTIES;

        // 拦截器链
        final List<String> list = new ArrayList<>();
        list.add(TimeInterceptor.class.getName());
        list.add(CounterInterceptor.class.getName());

        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, list);

        // 空指针异常
//        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
//            for (int i = 0; i < 10; i++) {
//                kafkaProducer.send(new ProducerRecord<>("first", "test" + i));
//            }
//        }

        final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            try {
                kafkaProducer.send(new ProducerRecord<>("first", "test" + i));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        kafkaProducer.close();
    }

    public static class TimeInterceptor implements ProducerInterceptor<String, String> {

        /**
         * 该方法封装进 KafkaProducer.send 方法中，即它运行在用户主线程中。Producer 确保在
         * 消息被序列化以及计算分区前调用该方法。用户可以在该方法中对消息做任何操作，但最好
         * 保证不要修改消息所属的 topic 和分区，否则会影响目标分区的计算。
         *
         * @param record
         * @return
         */
        @Override
        public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
            return new ProducerRecord<>(record.topic(), record.key(), System.currentTimeMillis() + "," + record.value());
        }

        /**
         * 该方法会在消息从 RecordAccumulator 成功发送到 Kafka Broker 之后，或者在发送过程
         * 中失败时调用。并且通常都是在 producer 回调逻辑触发之前。onAcknowledgement 运行在
         * producer 的 IO 线程中，因此不要在该方法中放入很重的逻辑，否则会拖慢 producer 的消息
         * 发送效率。
         *
         * @param metadata
         * @param exception
         */
        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

        }

        /**
         * 关闭 interceptor，主要用于执行一些资源清理工作
         * 如前所述，interceptor 可能被运行在多个线程中，因此在具体实现时用户需要自行确保
         * 线程安全。另外倘若指定了多个 interceptor，则 producer 将按照指定顺序调用它们，并仅仅
         * 是捕获每个 interceptor 可能抛出的异常记录到错误日志中而非在向上传递。这在使用过程中
         * 要特别留意。
         */
        @Override
        public void close() {

        }

        /**
         * 获取配置信息和初始化数据时调用
         *
         * @param configs
         */
        @Override
        public void configure(Map<String, ?> configs) {

        }
    }

    public static class CounterInterceptor implements ProducerInterceptor<String, String> {
        private int countSuccess = 0;
        private int countFail = 0;

        @Override
        public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                countFail++;
            } else {
                countSuccess++;
            }
        }

        @Override
        public void close() {
            System.out.println("成功发送 " + countSuccess);
            System.out.println("发送失败 " + countFail);
        }

        @Override
        public void configure(Map<String, ?> configs) {

        }
    }
}

