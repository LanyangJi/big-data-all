package cn.jly.elasticsearch;

import cn.jly.elasticsearch.beans.Utils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * 请求体查询
 *
 * @author lanyangji
 * @date 2021/4/10 下午 2:38
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch11RequstBodyQuery
 */
public class Elasticsearch11RequestBodyQuery extends Utils {
    public static void main(String[] args) {
        try (RestHighLevelClient client =
                     new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))) {
            // 创建搜索请求对象
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("user");
            // 构建查询的请求体 - 查询所有数据
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchRequest.source(searchSourceBuilder);

            // 发送请求并返回
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            // 打印结果
            System.out.println("_took: " + searchResponse.getTook());
            System.out.println("_timeout: " + searchResponse.isTimedOut());
            SearchHits hits = searchResponse.getHits();
            System.out.println("_total: " + hits.getTotalHits());
            System.out.println("_maxScore: " + hits.getMaxScore());
            System.out.println("_hits -> ");
            for (SearchHit hit : hits) {
                System.out.println(hit.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
