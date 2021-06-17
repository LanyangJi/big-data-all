package cn.jly.scala

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature

import scala.beans.BeanProperty

/**
 * @author jilanyang
 * @date 2021/5/27 0027 17:21
 * @packageName cn.jly.scala 
 * @className JsonTest
 */
object JsonTest {

  def main(args: Array[String]): Unit = {
    val person: Person = new Person
    person.setName("tom")
    person.setAge(23)

    val json: String = JSON.toJSONString(person, SerializerFeature.PrettyFormat)
    println(json)

    val bean: Person = JSON.parseObject(json, classOf[Person])
    println(bean)
  }

  class Person {
    @BeanProperty
    var name: String = _
    @BeanProperty
    var age: Int = 0

    override def toString = s"Person(name=$name, age=$age)"
  }

}

