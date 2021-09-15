package cn.jly.bigdata.flink.datastream.c02_source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jilanyang
 * @date 2021/6/30 0030 13:19
 * @packageName cn.jly.bigdata.flink.datastream.c02_source
 * @className D02_ReadTextFile
 */
public class D02_ReadTextFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.readTextFile(本地/HDFS文件/文件夹);//压缩文件也可以
        DataStreamSource<String> inputDataStream = env.readTextFile("input/word.txt");

        inputDataStream.print();

        env.execute("D02_ReadTextFile");
    }
}
