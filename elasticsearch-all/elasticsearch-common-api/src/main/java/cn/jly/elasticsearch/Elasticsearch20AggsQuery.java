package cn.jly.elasticsearch;

import cn.jly.elasticsearch.beans.Utils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * 聚合查询
 *
 * @author lanyangji
 * @date 2021/4/12 下午 2:42
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch20AggsQuery
 */
public class Elasticsearch20AggsQuery extends Utils {
    public static void main(String[] args) {
        try (
                RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))
        ) {
            SearchRequest request = new SearchRequest().indices("student");

            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.aggregation(AggregationBuilders.max("maxAge").field("age"));
            // 不查看记录，只看聚合结果
            builder.size(0);

            request.source(builder);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            System.out.println(OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(response));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
