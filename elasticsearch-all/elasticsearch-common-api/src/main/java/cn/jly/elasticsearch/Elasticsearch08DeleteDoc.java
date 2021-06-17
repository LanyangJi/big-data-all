package cn.jly.elasticsearch;

import cn.jly.elasticsearch.beans.Utils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * 删除文档
 *
 * @author lanyangji
 * @date 2021/4/10 下午 1:20
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch08DeleteDoc
 */
public class Elasticsearch08DeleteDoc extends Utils {
    public static void main(String[] args) {
        try (RestHighLevelClient client =
                     new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))) {
            // 删除请求
            DeleteRequest deleteRequest = new DeleteRequest().index("user").id("1001");
            // 发送请求并获得响应
            DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
            System.out.println("删除： " + deleteResponse.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
