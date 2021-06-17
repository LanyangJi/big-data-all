package cn.jly.elasticsearch;

import cn.hutool.core.lang.Console;
import cn.jly.elasticsearch.beans.Utils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * 删除索引
 *
 * @author lanyangji
 * @date 2021/4/9 下午 5:16
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch04DeleteIndex
 */
public class Elasticsearch04DeleteIndex extends Utils {
    public static void main(String[] args) {
        try (RestHighLevelClient client =
                     new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))) {
            // 删除索引请求
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("user");
            // 删除并获得响应
            AcknowledgedResponse response = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            Console.log("操作结果：" + response.isAcknowledged());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
