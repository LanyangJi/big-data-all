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
import org.elasticsearch.search.sort.SortOrder;

/**
 * 排序查询
 *
 * @author lanyangji
 * @date 2021/4/12 下午 1:53
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch14SortQuery
 */
public class Elasticsearch14SortQuery extends Utils {
    public static void main(String[] args) {
        try (RestHighLevelClient client =
                     new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))) {
            // 查询请求
            SearchRequest request = new SearchRequest();
            request.indices("user");
            // 查询请求体
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.matchAllQuery());
            builder.sort("age", SortOrder.DESC);
            request.source(builder);

            // 发送请求并获得返回
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 打印结果
            System.out.println("_took: " + response.getTook());
            System.out.println("_timeout: " + response.isTimedOut());
            SearchHits hits = response.getHits();
            System.out.println("_total: " + hits.getTotalHits());
            System.out.println("_maxScore: " + hits.getMaxScore());
            System.out.println("_hits -> ");
            for (SearchHit hit : hits) {
                System.out.println(hit.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
