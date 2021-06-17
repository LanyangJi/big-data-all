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
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

/**
 * 高亮查询
 *
 * @author lanyangji
 * @date 2021/4/12 下午 2:33
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch19HightlightQuery
 */
public class Elasticsearch19HighlightQuery extends Utils {
    public static void main(String[] args) {
        try (
                RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))
        ) {
            SearchRequest request = new SearchRequest();
            request.indices("student");

            // 查询请求体构造器
            SearchSourceBuilder builder = new SearchSourceBuilder();
            // 查询方式
            builder.query(QueryBuilders.termQuery("name", "zhangsan"));
            // 高亮字段
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            highlightBuilder.preTags("<font color='red'>");
            highlightBuilder.postTags("</font>");
            highlightBuilder.field("name");
            builder.highlighter(highlightBuilder);

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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
