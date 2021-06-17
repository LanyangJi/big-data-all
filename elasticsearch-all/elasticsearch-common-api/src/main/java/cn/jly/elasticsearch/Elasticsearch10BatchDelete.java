package cn.jly.elasticsearch;

import cn.jly.elasticsearch.beans.Utils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @author lanyangji
 * @date 2021/4/10 下午 1:50
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch10BatchDelete
 */
public class Elasticsearch10BatchDelete extends Utils {
    public static void main(String[] args) {
        try (RestHighLevelClient client =
                     new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))) {
            // 批量请求对象
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new DeleteRequest().index("user").id("1001"))
                    .add(new DeleteRequest().index("user").id("1002"));
            // 发送请求
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            // 打印结果
            System.out.println("_took: " + bulkResponse.getTook());
            System.out.println("_items: " + bulkResponse.getItems());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
