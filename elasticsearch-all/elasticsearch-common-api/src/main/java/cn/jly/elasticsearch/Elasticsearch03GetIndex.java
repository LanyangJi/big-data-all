package cn.jly.elasticsearch;

import cn.hutool.core.lang.Console;
import cn.jly.elasticsearch.beans.Utils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 查看索引
 *
 * @author lanyangji
 * @date 2021/4/9 上午 10:20
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch03GetIndex
 */
public class Elasticsearch03GetIndex extends Utils {
    public static void main(String[] args) {
        try (RestHighLevelClient client =
                     new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))) {
            // 查看索引请求
            GetIndexRequest getIndexRequest = new GetIndexRequest("user");
            // 请求并获得响应
            GetIndexResponse getIndexResponse = client.indices().get(getIndexRequest, RequestOptions.DEFAULT);
            // 别名
            Map<String, List<AliasMetadata>> aliases = getIndexResponse.getAliases();
            Console.log(aliases);
            // 映射
            Map<String, MappingMetadata> mappings = getIndexResponse.getMappings();
            Console.log(mappings);
            // 设置
            Map<String, Settings> settings = getIndexResponse.getSettings();
            Console.log(settings);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
