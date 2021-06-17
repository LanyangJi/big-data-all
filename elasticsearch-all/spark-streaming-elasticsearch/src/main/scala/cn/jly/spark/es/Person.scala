package cn.jly.spark.es

import com.fasterxml.jackson.databind.ObjectMapper

/**
 * @author lanyangji
 * @date 2021/4/14 下午 8:52
 * @packageName
 * @className cn.jly.spark.es.Person
 */
case class Person(id: Long, name: String, age: Int, email: String) extends Serializable {
}