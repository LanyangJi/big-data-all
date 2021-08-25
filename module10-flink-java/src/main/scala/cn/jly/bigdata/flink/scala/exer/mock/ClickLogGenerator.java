package cn.jly.bigdata.flink.scala.exer.mock;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 点击流日志模拟器
 */
public class ClickLogGenerator {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        props.put("acks", "all");
        props.put("retries", 2);
        props.put("retries.backoff.ms", 20);
        props.put("buffer.memory", 33554432);
        props.put("batch.size", 16384);
        props.put("linger.ms", 25);
        props.put("max.request.size", 163840);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            //构建log
            ClickLog clickLog = new ClickLog();
            clickLog.setChannelID(channelID[random.nextInt(channelID.length)]);
            clickLog.setCategoryID(categoryID[random.nextInt(categoryID.length)]);
            clickLog.setProduceID(produceID[random.nextInt(produceID.length)]);
            clickLog.setUserID(userID[random.nextInt(userID.length)]);
            clickLog.setCountry(contrys[random.nextInt(contrys.length)]);
            clickLog.setProvince(provinces[random.nextInt(provinces.length)]);
            clickLog.setCity(citys[random.nextInt(citys.length)]);
            clickLog.setNetwork(networks[random.nextInt(networks.length)]);
            clickLog.setSource(sources[random.nextInt(sources.length)]);
            clickLog.setBrowserType(browser[random.nextInt(browser.length)]);
            Long[] times = usetimelog.get(random.nextInt(usetimelog.size()));
            clickLog.setEntryTime(times[0]);
            clickLog.setLeaveTime(times[1]);
            String logJson = JSONObject.toJSONString(clickLog);
            //构建msg
            Message msg = new Message();//构建Message
            msg.setMessage(logJson);
            msg.setCount(1);
            msg.setTimeStamp(System.currentTimeMillis());
            String msgJson = JSON.toJSONString(msg);
            //发送数据
            ProducerRecord<String, String> record = new ProducerRecord<>("pyg", msgJson);
            kafkaProducer.send(record);

            System.out.println("消息已发送到Kafka:"+msgJson);
            Thread.sleep(500);
        }
        kafkaProducer.close();
    }

    private static Long[] channelID = new Long[]{1l,2l,3l,4l,5l,6l,7l,8l,9l,10l,11l,12l,13l,14l,15l,16l,17l,18l,19l,20l};//频道id集合
    private static Long[] categoryID = new Long[]{1l,2l,3l,4l,5l,6l,7l,8l,9l,10l,11l,12l,13l,14l,15l,16l,17l,18l,19l,20l};//产品类别id集合
    private static Long[] produceID = new Long[]{1l,2l,3l,4l,5l,6l,7l,8l,9l,10l,11l,12l,13l,14l,15l,16l,17l,18l,19l,20l};//产品id集合
    private static Long[] userID = new Long[]{1l,2l,3l,4l,5l,6l,7l,8l,9l,10l,11l,12l,13l,14l,15l,16l,17l,18l,19l,20l};//用户id集合

    /**
     * 地区
     */
    private static String[] contrys = new String[]{"china"};//地区-国家集合
    private static String[] provinces = new String[]{"HeNan","HeBei"};//地区-省集合
    private static String[] citys = new String[]{"ShiJiaZhuang","ZhengZhou", "LuoYang"};//地区-市集合

    /**
     *网络方式
     */
    private static String[] networks = new String[]{"电信","移动","联通"};

    /**
     * 来源方式
     */
    private static String[] sources = new String[]{"直接输入","百度跳转","360搜索跳转","必应跳转"};

    /**
     * 浏览器
     */
    private static String[] browser = new String[]{"火狐","qq浏览器","360浏览器","谷歌浏览器"};

    /**
     * 打开时间 离开时间
     */
    private static List<Long[]> usetimelog = producetimes();
    //获取时间
    public static List<Long[]> producetimes(){
        List<Long[]> usetimelog = new ArrayList<Long[]>();
        for(int i=0;i<100;i++){
            Long [] timesarray = gettimes("2020-01-01 24:60:60:000");
            usetimelog.add(timesarray);
        }
        return usetimelog;
    }

    private static Long [] gettimes(String time){
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
        try {
            Date date = dateFormat.parse(time);
            long timetemp = date.getTime();
            Random random = new Random();
            int randomint = random.nextInt(10);
            long starttime = timetemp - randomint*3600*1000;
            long endtime = starttime + randomint*3600*1000;
            return new Long[]{starttime,endtime};
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new Long[]{0L,0L};
    }

    @Data
    public static class Message {
        // 消息次数
        private int count;
        // 消息的时间戳
        private long timeStamp;
        // 消息体
        private String message;
    }

    /**
     * 点击流日志
     */
    @Data
    public static class ClickLog {
        //频道ID
        private long channelID ;
        //产品的类别ID
        private long categoryID ;
        //产品ID
        private long produceID ;
        //用户的ID
        private long userID ;
        //国家
        private String country ;
        //省份
        private String province ;
        //城市
        private String city ;
        //网络方式
        private String network ;
        //来源方式
        private String source ;
        //浏览器类型
        private String browserType;
        //进入网站时间
        private Long entryTime ;
        //离开网站时间
        private long leaveTime ;
    }
}
