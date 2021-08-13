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
@NoArgsConstructor
@AllArgsConstructor
public class Customer {
    private String id;
    private String name;
    private Integer age;
}
