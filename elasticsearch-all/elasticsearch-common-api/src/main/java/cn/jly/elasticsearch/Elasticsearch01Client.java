package cn.jly.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * 测试连接
 * @author lanyangji
 * @date 2021年4月9日09:58:12
 */
public class Elasticsearch01Client {
    public static void main(String[] args) {
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("linux01", 9200, "http")))) {
            IndicesClient indices = client.indices();
            System.out.println("indices = " + indices);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
