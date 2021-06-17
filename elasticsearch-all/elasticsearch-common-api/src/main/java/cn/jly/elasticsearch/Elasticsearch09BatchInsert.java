package cn.jly.elasticsearch;

import cn.hutool.core.util.RandomUtil;
import cn.jly.elasticsearch.beans.User;
import cn.jly.elasticsearch.beans.Utils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 批量新增
 *
 * @author lanyangji
 * @date 2021/4/10 下午 1:25
 * @packageName cn.jly.elasticsearch
 * @className Elasticsearch09BatchInsert
 */
public class Elasticsearch09BatchInsert extends Utils {
    public static void main(String[] args) {
        try (RestHighLevelClient client =
                     new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA)))) {
            // 批量请求
            BulkRequest bulkRequest = new BulkRequest();
            for (User user : getUsers(5)) {
                bulkRequest.add(
                        new IndexRequest().index("user").id(user.getId().toString()).source(toJsonStr(user), XContentType.JSON)
                );
            }
            // 发送批量添加请求，并获得响应
            BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            System.out.println("took = " + response.getTook());
            System.out.println("items: " + response.getItems());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取用户列表
     *
     * @param count
     * @return
     */
    private static List<User> getUsers(int count) {
        ArrayList<User> list = new ArrayList<>();
        if (count < 1) {
            return list;
        }

        for (int i = 0; i < count; i++) {
            long id = 1010L + i;
            list.add(new User(id, RandomUtil.randomString(5), RandomUtil.randomInt(100), RandomUtil.randomEle(Arrays.asList("男", "女"))));
        }
        return list;
    }
}
