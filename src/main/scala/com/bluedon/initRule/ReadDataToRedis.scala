package com.bluedon.initRule

import java.util
import java.util.concurrent.TimeUnit

import net.sf.json.{JSONArray, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

/**
  * 读取pgsql规则库数据存入redis中
  * Created by Administrator on 2017/6/15 0015.
  */
class ReadDataToRedis {

  /**
    * 定时的更新日志规则,从pgSQL中读取数据存入redis中
    * @param sparkSession
    * @param jedis
    * @param pgUrl pgSQL url
    * @param pgUsername
    * @param pgPassword
    */
  def startRefreshDataToRedis(sparkSession: SparkSession,jedis: Jedis,pgUrl:String,pgUsername:String,pgPassword:String):Unit={
    dealEventRegexRule(sparkSession,jedis,pgUrl,pgUsername,pgPassword)
    dealLogRegexRule(sparkSession,jedis,pgUrl,pgUsername,pgPassword)
  }


  /**
    * 保存数据进redis
    * @param sparkSession
    * @param jedis
    * @param ruleName
    * @param jdbcDF
    */
    def readDataToRedis(sparkSession: SparkSession,jedis: Jedis,ruleName:String,jdbcDF:DataFrame):Unit={

      val cols: Array[String] = jdbcDF.columns
      val ruleArray:JSONArray = new JSONArray()
      jdbcDF.collect().foreach(row=>{
        val ruleJson = new JSONObject()
        var rowjson = ""
        for (index<-0  to cols.length-1){
          var field = row.get(index)
          if(StringUtils.isEmpty(String.valueOf(field)) || "null".equals(field) || null == field){
            field = ""
          }
          ruleJson.put(cols(index),String.valueOf(field))
        }
        ruleArray.add(ruleJson)
      })

      jedis.set(ruleName,ruleArray.toString)
    }

  /**
    * 定义规则
     * @param sparkSession
    * @param jedis
    * @param url
    * @param username
    * @param password
    */
    def dealEventRegexRule(sparkSession: SparkSession,jedis: Jedis,url:String,username:String,password:String):Unit={

      val evenrr = new EventRegexRule

      val df2 = evenrr.getEventDefBySystem(sparkSession,url,username,password)
      val ruleName2 = "eventDefBySystem"
      readDataToRedis(sparkSession,jedis,ruleName2,df2)

      val df3 = evenrr.getEventRule(sparkSession,url,username,password)
      val ruleName3 = "eventRule"
      readDataToRedis(sparkSession,jedis,ruleName3,df3)
    }

  /**
    * 原始日志规则-内置
    * @param sparkSession
    * @param jedis
    * @param url
    * @param username
    * @param password
    */
    def dealLogRegexRule(sparkSession: SparkSession,jedis: Jedis,url:String,username:String,password:String):Unit={
      val df = new LogRegexRule().getLogRegex(sparkSession,url,username,password)
      val ruleName = "logRegex"
      readDataToRedis(sparkSession,jedis,ruleName,df)
    }




}
























