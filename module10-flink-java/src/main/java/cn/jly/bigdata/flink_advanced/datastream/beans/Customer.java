package cn.jly.bigdata.flink_advanced.datastream.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 消费者类
 * @author jilanyang
 * @createTime 2021/8/13 16:22
 */
@Data
public class Customer {
    private String id;
    private String name;
    private Integer age;
    private Long createTime;

    public Customer() {
    }

    public Customer(String id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public Customer(String id, String name, Integer age, Long createTime) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.createTime = createTime;
    }
}
