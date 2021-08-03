package cn.jly.bigdata.flink_advanced.datastream.c02_source;

import cn.jly.bigdata.flink_advanced.utils.StreamWordCountUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * env.readTextFile(本地文件[夹]/HDFS文件[夹])
 * 注意：压缩文件也可以
 *
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.datastream.c02_source
 * @class D02_ReadTextFile
 * @date 2021/7/25 17:17
 */
public class D02_ReadTextFile {
    public static void main(String[] args) throws Exception {
        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 读取本地文件
        DataStreamSource<String> fileDS = env.readTextFile("input/word.txt");

        // wordcount
        StreamWordCountUtils.wordCount(fileDS, " ")
                .print();

        env.execute("D02_ReadTextFile");
    }
}
