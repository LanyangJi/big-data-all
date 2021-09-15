package cn.jly.bigdata.project.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author jilanyang
 * @date 2021/8/29 12:29
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ItemViewCount implements Serializable {
    private Long itemId;
    private Long windowEnd;
    private Long count;
}
