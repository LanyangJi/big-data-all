package cn.jly.elasticsearch.beans;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.text.SimpleDateFormat;

/**
 * @author lanyangji
 * @date 2021/4/9 上午 10:20
 * @packageName cn.jly.elasticsearch
 * @className Constants
 */
public class Utils {
    public static final String HOSTNAME = "linux01";
    public static final int PORT = 9200;
    public static final String SCHEMA = "http";
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        //在反序列化时忽略在 json 中存在但 Java 对象不存在的属性
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        //在序列化时日期格式默认为 yyyy-MM-dd'T'HH:mm:ss.SSSZ
        OBJECT_MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        //在序列化时自定义时间日期格式
        OBJECT_MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        //在序列化时忽略值为 null 的属性
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public static String toJsonStr(Object obj) throws Exception {
        return OBJECT_MAPPER.writeValueAsString(obj);
    }

    public static void main(String[] args) throws Exception {
        User user = new User(1001L, "zhangsan", 23, "男");
        String userJson = toJsonStr(user);
        System.out.println("userJson = " + userJson);
    }
}
