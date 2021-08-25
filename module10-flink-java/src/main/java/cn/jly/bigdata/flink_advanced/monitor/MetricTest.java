package cn.jly.bigdata.flink_advanced.monitor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

/**
 * 使用 flink REST API的方式，通过http请求实时获取flink任务状态，不是RUNNING状态则进行短信、电话或邮件报警，达到实时监控的效果。
 *
 * @author jilanyang
 * @date 2021/8/24 21:26
 */
public class MetricTest {

    public static void main(String[] args) {
        String result = sendGet("http://linux01:8081/jobs/7a8db9ab44f325ae983cffc144416a1f/vertices/20ba6b65f97481d5570070de90e4e791/metrics?get=0.Map.flink_test_metric.mapDataNum,1.Map.flink_test_metric.mapDataNum,0.buffers.inPoolUsage");
        System.out.println(result);
    }

    public static String sendGet(String url) {
        String result = "";
        BufferedReader in = null;
        try {
            URL realUrl = new URL(url);
            URLConnection connection = realUrl.openConnection();
            // 设置通用的请求属性
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.connect();
            in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送GET请求出现异常！" + e);
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }
}