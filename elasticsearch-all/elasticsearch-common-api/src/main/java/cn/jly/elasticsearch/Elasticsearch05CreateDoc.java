package cn.jly.elasticsearch;

import cn.hutool.core.lang.Console;
import cn.jly.elasticsearch.beans.User;
import cn.jly.elasticsearch.beans.Utils;
import lombok.val;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * 创建文档
 *
 * @author lanyangji
 * @date 2021/4/10 上午 10:36
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch05CreateDoc
 */
public class Elasticsearch05CreateDoc extends Utils {
    public static void main(String[] args) {
        try(val client = new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))) {
            // 创建数据对象
            User user = new User(1001L, "zhangsan", 23, "男");
            String userJson = toJsonStr(user);

            // index请求对象
            IndexRequest indexRequest = new IndexRequest();
            // 设置索引以及唯一标识
            indexRequest.index("user").id(String.valueOf(user.getId()));
            // 添加文档数据
            indexRequest.source(userJson, XContentType.JSON);
            // 客户端发送请求，并获得响应
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            // 打印结果
            Console.log("_index: " + indexResponse.getIndex());
            Console.log("_id: " + indexResponse.getId());
            Console.log("_result: " + indexResponse.getResult());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
