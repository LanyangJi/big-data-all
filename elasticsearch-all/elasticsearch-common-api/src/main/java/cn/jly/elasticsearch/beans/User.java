package cn.jly.elasticsearch.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lanyangji
 * @date 2021/4/10 上午 10:34
 * @packageName cn.jly.elasticsearch.beans
 * @className User
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {
    private Long id;
    private String name;
    private Integer age;
    private String sex;
}
