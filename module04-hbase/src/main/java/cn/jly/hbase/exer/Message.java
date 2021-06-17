package cn.jly.hbase.exer;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Administrator
 * @date 2021/5/26 0026 9:58
 * @packageName cn.jly.hbase.cn.spark.core.exer
 * @className Message
 */
@NoArgsConstructor
@Data
public class Message {
    private String userId;
    private String content;
    private String timestamp;
}
