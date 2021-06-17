package cn.jly.elasticsearch;

import cn.hutool.core.lang.Console;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;

import java.io.IOException;

/**
 * 创建索引
 *
 * @author lanyangji
 * @date 2021/4/9 上午 10:14
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch02CreateIndex
 */
public class Elasticsearch02CreateIndex {
    public static void main(String[] args) {
        // 发送请求，获取响应
        try (RestHighLevelClient client =
                     new RestHighLevelClient(RestClient.builder(new HttpHost("linux01", 9200, "http")))) {
            // 创建索引请求对象
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("user");
            CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            // 响应状态
            boolean acknowledged = createIndexResponse.isAcknowledged();
            Console.log("操作状态 ->" + acknowledged);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
