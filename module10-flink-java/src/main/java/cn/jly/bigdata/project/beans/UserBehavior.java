package cn.jly.bigdata.project.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 用户行为pojo
 *
 * @author jilanyang
 * @date 2021/8/29 12:26
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior implements Serializable {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;
}
