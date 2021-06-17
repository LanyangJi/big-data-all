package cn.jly.elasticsearch;

import cn.jly.elasticsearch.beans.Utils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @author lanyangji
 * @date 2021/4/10 上午 11:43
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch07GetDoc
 */
public class Elasticsearch07GetDoc extends Utils {
    public static void main(String[] args) {
        try (RestHighLevelClient client =
                     new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))) {
            // 查看文档请求
            GetRequest getRequest = new GetRequest().index("user").id("1001");
            // 发送请求并获得响应
            GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
            // 打印结果
            System.out.println("_index: \t" + getResponse.getIndex());
            System.out.println("_type: \t" + getResponse.getType());
            System.out.println("_id: \t" + getResponse.getId());
            System.out.println("_source: \t" + getResponse.getSource());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
