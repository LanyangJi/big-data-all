package cn.jly.spark.es

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.apache.http.HttpHost
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicLong

/**
 * @author lanyangji
 * @date 2021/4/14 下午 8:43
 * @packageName
 * @className SparkStreamingElasticsearchDemo
 */
object SparkStreamingElasticsearchDemo {
  var idGenerator:AtomicLong = new AtomicLong(1000);

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingElasticsearchDemo")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    var client: RestHighLevelClient = null
    val ds: ReceiverInputDStream[String] = streamingContext.socketTextStream("linux01", 9999)
    ds.foreachRDD {
      rdd: RDD[String] => {
        println("********** " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
        rdd.foreach {
          data: String => {
            try {
              if (data.trim.nonEmpty) {
                // 新增文档请求
                val request: IndexRequest = new IndexRequest().index("persons").id(idGenerator.getAndIncrement().toString)
                request.source(data, XContentType.JSON)

                // client
                client = new RestHighLevelClient(RestClient.builder(new HttpHost("linux01", 9200, "http")))
                val response: IndexResponse = client.index(request, RequestOptions.DEFAULT)

                // 打印响应
                println("_index: " + response.getIndex)
                println("_id: " + response.getId)
                println("_result: " + response.getResult)
              }
            } catch {
              case ex: Exception => ex.printStackTrace()
            } finally {
              if (client != null)
                client.close()
            }
          }
        }
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
