package cn.jly.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Administrator
 * @date 2021/5/27 0027 10:08
 * @packageName cn.jly.kafka
 * @className KafkaUtils
 */
public class KafkaUtils {
    public static final Properties PRODUCER_CONFIG_PROPERTIES;
    public static final Properties CONSUMER_CONFIG_PROPERTIES;

    private static final String BOOTSTRAP_SERVERS = "linux01:9092";

    static {
        PRODUCER_CONFIG_PROPERTIES = new Properties();
        // kafka集群地址
        PRODUCER_CONFIG_PROPERTIES.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // ack规则
        PRODUCER_CONFIG_PROPERTIES.put(ProducerConfig.ACKS_CONFIG, "all");
        // 失败重试次数
        PRODUCER_CONFIG_PROPERTIES.put(ProducerConfig.RETRIES_CONFIG, 2);
        // 批次大小
        PRODUCER_CONFIG_PROPERTIES.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 等待时间
        PRODUCER_CONFIG_PROPERTIES.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // recordAccumulator大小
        PRODUCER_CONFIG_PROPERTIES.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        PRODUCER_CONFIG_PROPERTIES.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        PRODUCER_CONFIG_PROPERTIES.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        CONSUMER_CONFIG_PROPERTIES = new Properties();
        CONSUMER_CONFIG_PROPERTIES.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        CONSUMER_CONFIG_PROPERTIES.put(ConsumerConfig.GROUP_ID_CONFIG, "lanyangji");
        // 自定提交offset
        CONSUMER_CONFIG_PROPERTIES.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 自动提交offset的时间间隔
        CONSUMER_CONFIG_PROPERTIES.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        CONSUMER_CONFIG_PROPERTIES.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        CONSUMER_CONFIG_PROPERTIES.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    /**
     * 发送消息
     *
     * @param topic
     * @param message
     */
    public static void send(String topic, String message) {
        try (
                final KafkaProducer<String, String> producer = new KafkaProducer<>(PRODUCER_CONFIG_PROPERTIES)
        ) {
            producer.send(new ProducerRecord<>(topic, message));
        }
    }

    /**
     * 带回调函数的发送消息
     *
     * @param topic
     * @param message
     * @param callback
     * @return
     */
    public static Future<RecordMetadata> sendWithCallback(String topic, String message, Callback callback) {
        try (
                final KafkaProducer<String, String> producer = new KafkaProducer<>(PRODUCER_CONFIG_PROPERTIES)
        ) {
            return producer.send(new ProducerRecord<>(topic, message), callback);
        }
    }

    /**
     * 同步发送消息
     * 消息发送之后会阻塞线程，直至ack返回
     *
     * @param topic
     * @param message
     */
    public static void SendSync(String topic, String message) throws ExecutionException, InterruptedException {
        try (final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(PRODUCER_CONFIG_PROPERTIES)) {
            kafkaProducer.send(new ProducerRecord<>(topic, message)).get();
        }
    }

    /**
     * 同步带回调
     *
     * @param topic
     * @param message
     * @param callback
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void SendSyncWithCallback(String topic, String message, Callback callback) throws ExecutionException, InterruptedException {
        try (final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(PRODUCER_CONFIG_PROPERTIES)) {
            kafkaProducer.send(new ProducerRecord<>(topic, message), callback).get();
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 发消息
        // send("first", "wanghaitao");

        // 带回调函数发送消息
//        sendWithCallback("first", "test-value", (recordMetadata, e) -> {
//            System.out.println("回调逻辑 -> " + recordMetadata);
//            if (e != null) {
//                e.printStackTrace();
//            }
//        });

        // 同步发送
        // SendSync("first", "testVal");

        // 消费消息，自定提交offset, enable.auto.commit=true
//        final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(CONSUMER_CONFIG_PROPERTIES);
//        kafkaConsumer.subscribe(Collections.singletonList("first"));
//        while (true){
//            final ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.println(record.topic());
//                System.out.println(record.value());
//                System.out.println(record.offset());
//            }
//        }

        // 消费消息，同步的方式手动提交offset, enable.auto.commit=false
//        final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(CONSUMER_CONFIG_PROPERTIES);
//        kafkaConsumer.subscribe(Collections.singletonList("first"));
//        while (true) {
//            final ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.println(record.topic());
//                System.out.println(record.value());
//                System.out.println(record.offset());
//
//                // 同步提交，会阻塞线程，直到提交成功
//                kafkaConsumer.commitSync();
//            }
//        }

        // 异步手动提交offset, 虽然同步提交 offset 更可靠一些，但是由于其会阻塞当前线程，直到提交成功。因此吞
        //吐量会收到很大的影响。因此更多的情况下，会选用异步提交 offset 的方式。
        final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(CONSUMER_CONFIG_PROPERTIES);
        kafkaConsumer.subscribe(Collections.singletonList("first"));
        while (true){
            final ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.topic());
                System.out.println(record.value());
                System.out.println(record.offset());

                // 异步提交
                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        System.out.println("异步提交");
                        if (exception != null) {
                            exception.printStackTrace();
                        }
                    }
                });
            }
        }
    }
}
