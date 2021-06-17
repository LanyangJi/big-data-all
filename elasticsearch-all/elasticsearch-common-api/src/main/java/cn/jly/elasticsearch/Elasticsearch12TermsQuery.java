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
 * terms查询，查询条件为关键字
 *
 * @author lanyangji
 * @date 2021/4/10 下午 7:50
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch12TermsQuery
 */
public class Elasticsearch12TermsQuery extends Utils {
    public static void main(String[] args) {
        try (RestHighLevelClient client =
                     new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))) {
            // 搜索请求对象
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("student");

            // 构建查询请求体
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.termQuery("age", "30"));
            searchRequest.source(searchSourceBuilder);

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
