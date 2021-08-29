package cn.jly.bigdata.project.utils;

import cn.hutool.core.util.RandomUtil;
import cn.jly.bigdata.project.beans.UserBehavior;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.tool.PreUpgradeValidator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

/**
 * @author jilanyang
 * @date 2021/8/29 21:24
 */
public class KafkaUtils {

    private static Properties properties;
    private static KafkaProducer<String, String> kafkaProducer;

    static {
        properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux01:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    @SneakyThrows
    public static void main(String[] args) {
        List<String> userBehaviorActions = Arrays.asList(
                "pv", "cart", "buy", "fav"
        );

        while (true) {
            UserBehavior behavior = new UserBehavior();
            behavior.setUserId(RandomUtil.randomLong(100001, 100100));
            behavior.setItemId(RandomUtil.randomLong(100, 120));
            behavior.setCategoryId(RandomUtil.randomInt(behavior.getItemId().intValue()));
            behavior.setBehavior(userBehaviorActions.get(RandomUtil.randomInt(0, userBehaviorActions.size())));
            behavior.setTimestamp(System.currentTimeMillis());

            writeToKafka("test", JSON.toJSONString(behavior));

            Thread.sleep(1000L);
        }
    }

    /**
     * 将数据写入kafka
     *
     * @param topic
     * @param data
     */
    public static void writeToKafka(String topic, String data) {
        kafkaProducer.send(new ProducerRecord<>(topic, data));
    }

}
