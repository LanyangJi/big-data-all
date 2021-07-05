package cn.jly.bigdata.flink.datastream.c02_source;

import cn.jly.bigdata.flink.datastream.beans.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

/**
 * @author jilanyang
 * @date 2021/7/1 15:05
 * @packageName cn.jly.bigdata.flink.datastream.c02_source
 * @className D04_CustomSource
 */
public class D04_CustomSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SocketSource("linux01", 9999))
                .print();

        env.execute("D04_CustomSource");
    }

    /**
     * 自定义数据源实现从端口读取数据
     */
    public static class SocketSource implements SourceFunction<SensorReading> {
        private boolean isRunning = true;

        private final String hostname;
        private final int port;

        public SocketSource(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
        }

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            try (
                    Socket socket = new Socket(this.hostname, this.port);
                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()))
            ) {
                while (isRunning) {
                    String line = br.readLine();
                    if (StringUtils.isNotBlank(line)) {
                        SensorReading sensorReading = JSON.parseObject(line, SensorReading.class);
                        sourceContext.collect(sensorReading);
                    }

                    TimeUnit.MILLISECONDS.sleep(100);
                }
            }
        }

        /**
         * 取消
         */
        @Override
        public void cancel() {
            this.isRunning = false;
        }

        public String getHostname() {
            return hostname;
        }

        public int getPort() {
            return port;
        }
    }
}
