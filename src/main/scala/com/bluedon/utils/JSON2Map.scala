package com.bluedon.utils

import java.util

import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable

/**
  * Author: Dlin
  * Date:2017/8/24 15:20
  * Descripe: json字符串转为map工具类
  */
object JSON2Map {
  def json2Map(json : String) : mutable.HashMap[String,Object] = {

    val map : mutable.HashMap[String,Object]= mutable.HashMap()
    val jsonParser =new JSONParser()

    //将string转化为jsonObject
    val jsonObj: JSONObject = jsonParser.parse(json).asInstanceOf[JSONObject]

    //获取所有键
    val jsonKey = jsonObj.keySet()

    val iter = jsonKey.iterator()

    while (iter.hasNext){
      val field = iter.next()
      val value = jsonObj.get(field).toString
      if(value.startsWith("{")&&value.endsWith("}")){
        val value = mapAsScalaMap(jsonObj.get(field).asInstanceOf[util.HashMap[String, String]])
        map.put(field.toString,value)
      }else{
        map.put(field.toString,value)
      }
    }
    map
  }
}
