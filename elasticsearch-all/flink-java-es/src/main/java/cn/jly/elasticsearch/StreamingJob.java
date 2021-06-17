/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.jly.elasticsearch;

import cn.hutool.core.lang.Console;
import cn.hutool.json.JSONUtil;
import cn.jly.elasticsearch.beans.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Collections;
import java.util.List;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 * @author lanyangji
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.socketTextStream("linux01", 9999);

        // 转换处理
        SingleOutputStreamOperator<Person> personDs = ds.flatMap(new FlatMapFunction<String, Person>() {
            @Override
            public void flatMap(String value, Collector<Person> out) throws Exception {
                if (StringUtils.isNotEmpty(value)) {
                    String[] fields = value.split(",");
                    Person person = new Person();
                    person.setId(Long.parseLong(fields[0].trim()));
                    person.setName(fields[1].trim());
                    person.setAge(Integer.parseInt(fields[2]));
                    person.setEmail(fields[3].trim());

                    Console.log("输入 -> " + person);
                    out.collect(person);
                }
            }
        });

        // 构建sink
        List<HttpHost> httpHosts = Collections.singletonList(new HttpHost("linux01", 9200, "http"));
        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<Person> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Person>() {
                    public IndexRequest createIndexRequest(Person person) {
                        return Requests.indexRequest()
                                .index("flink-index")
                                .type("flink-es-type")
                                .id(String.valueOf(person.getId()))
                                .source(JSONUtil.toJsonStr(person), XContentType.JSON);
                    }
                    @Override
                    public void process(Person person, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(person));
                    }
                }
        );
        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);
        // provide a RestClientFactory for custom configuration on the internally created REST client
//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders(...)
//                    restClientBuilder.setMaxRetryTimeoutMillis(...)
//                    restClientBuilder.setPathPrefix(...)
//                    restClientBuilder.setHttpClientConfigCallback(...)
//                }
//        );

        // sink
        personDs.addSink(esSinkBuilder.build());

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
