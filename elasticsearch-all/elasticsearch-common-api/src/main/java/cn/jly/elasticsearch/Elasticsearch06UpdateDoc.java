package cn.jly.elasticsearch;

import cn.jly.elasticsearch.beans.Utils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * @author lanyangji
 * @date 2021/4/10 上午 11:17
 * @packageName cn.jly.elasticsearch.beans
 * @className Elasticsearch06UpdateDoc
 */
public class Elasticsearch06UpdateDoc extends Utils {
    public static void main(String[] args) {
        try (RestHighLevelClient client =
                     new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))) {
            // 修改请求
            UpdateRequest updateRequest = new UpdateRequest();
            // 设置索引和id
            updateRequest.index("user").id("1001");
            // 修改内容
            updateRequest.doc(XContentType.JSON, "sex", "女");
            // 发送请求，并获取响应
            UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
            System.out.println("_index:\t" + updateResponse.getIndex());
            System.out.println("_id:\t" + updateResponse.getId());
            System.out.println("_result:\t" + updateResponse.getResult());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
