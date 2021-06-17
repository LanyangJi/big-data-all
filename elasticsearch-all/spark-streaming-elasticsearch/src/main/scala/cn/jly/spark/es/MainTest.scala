package cn.jly.spark.es

import com.google.gson.Gson

/**
 * @author lanyangji
 * @date 2021/4/15 下午 8:54
 * @packageName cn.jly.spark.es 
 * @className MainTst
 */
object MainTest {
  def main(args: Array[String]): Unit = {
    val person: Person = Person(1001L, "tom", 23, "tom@qq.com")

    val gson: Gson = new Gson()
    val str: String = gson.toJson(person)
    println(str)

    val resPerson: Person = gson.fromJson(str, person.getClass)
    println(resPerson)
  }
}
