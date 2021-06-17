package cn.jly.elasticsearch.beans;

import lombok.Data;
import lombok.NoArgsConstructor;
import scala.compat.java8.collectionImpl.ProxySpliteratorViaNext;

/**
 * @author lanyangji
 * @date 2021/4/18 下午 10:08
 * @packageName cn.jly.elasticsearch.beans
 * @className Person
 */
@Data
@NoArgsConstructor
public class Person {
    private Long id;
    private String name;
    private Integer age;
    private String email;
}
