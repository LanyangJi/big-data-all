package cn.jly.elasticsearch;

import cn.jly.elasticsearch.beans.Utils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * bool查询
 *
 * @author lanyangji
 * @date 2021/4/12 下午 2:06
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch16BoolQuery
 */
public class Elasticsearch16BoolQuery extends Utils {
    public static void main(String[] args) {
        try (RestHighLevelClient client =
                     new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))) {
            SearchRequest request = new SearchRequest();
            request.indices("student");

            // 查询体
            SearchSourceBuilder builder = new SearchSourceBuilder();
            // bool查询构造器
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            // 必须包含
            boolQueryBuilder.must(QueryBuilders.matchQuery("age", "30"));
            // 一定不包含
            boolQueryBuilder.mustNot(QueryBuilders.matchQuery("name", "zhangsan"));
            // 可能包含
            boolQueryBuilder.should(QueryBuilders.matchQuery("sex", "男"));
            builder.query(boolQueryBuilder);

            request.source(builder);
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
