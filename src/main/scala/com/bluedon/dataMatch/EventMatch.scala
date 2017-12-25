package com.bluedon.dataMatch

import java.sql.{Connection, Statement}
import java.text.SimpleDateFormat
import java.util
import java.util.UUID

import com.bluedon.entity.TSiemGeneralLog
import com.bluedon.esinterface.config.ESClient
import com.bluedon.esinterface.index.IndexUtils
import com.bluedon.utils.{DateUtils, ROWUtils}
import net.sf.json
import net.sf.json.{JSONArray, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.control.Breaks._
import org.apache.spark.sql._
import org.apache.phoenix.spark._
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Client
import redis.clients.jedis.Jedis

/**
  * Created by dengxiwen on 2016/12/27.
  */
class EventMatch extends Serializable{

  /**
    * 新获取的规则,保存到redis中
    *
    * @param newEventDefId 新生成该规则id
    * @param matchlogs 原始日志范式化后的结果
    * @param eventDefRule 将要添加的规则集合
    * @param jedis
    */
  def saveEventRuleToRedis(newEventDefId: String, matchlogs: Array[String], eventDefRule: JSONArray, jedis: Jedis) = {

    val ruleName = "eventDefBySystem"
    var eventName = matchlogs(13).trim
    if(StringUtils.isEmpty(eventName)){
      eventName = ""
    }
    var eventLevel = matchlogs(14).trim
    if(StringUtils.isEmpty(eventLevel)){
      eventLevel = ""
    }
    val is_inner = "1"
    val is_use = "1"

    val jo = new JSONObject()
    jo.put("event_rule_id",newEventDefId)
    jo.put("event_rule_lib_id","")
    jo.put("event_rule_code","")
    jo.put("event_rule_name",eventName)
    jo.put("event_rule_level",eventLevel)
    jo.put("event_basic_type","")
    jo.put("event_sub_type","")
    jo.put("event_rule_desc","")
    jo.put("event_rule","")
    jo.put("is_inner",is_inner)
    jo.put("is_use",is_use)
    eventDefRule.add(jo.toString)
    jedis.set(ruleName,eventDefRule.toString)
  }

  /**
    * 拼装事件结果
    *
    * @param eventRuleResultId
    * @param eventDefId
    * @param eventDefName
    * @param eventRuleLevel
    * @param matchlog
    * @return
    */
  def getEventRuleResult(eventRuleResultId: String, eventDefId: String, eventDefName: String,
                         eventRuleLevel: String,matchlog:String):String = {
    var matchEvent:String = ""
    if(matchlog != null && !matchlog.trim.eq("") && matchlog.contains("~")) {
     // println("getEventRuleResult==="+matchlog)
      val matchlogs = matchlog.split("~")
      val eventtype = matchlogs(3).toString
      matchEvent = eventRuleResultId + "~" + eventRuleResultId + "~" + eventDefId + "~" + matchlogs(0).toString + "~" + eventDefName
      matchEvent += "~" + eventRuleLevel + "~" + matchlogs(4).toString + "~" + matchlogs(5).toString + "~" + matchlogs(6).toString
      matchEvent += "~" + matchlogs(7).toString + "~" + matchlogs(8).toString + "~" + matchlogs(2).toString + "~" + matchlogs(10).toString
      if (eventtype != null && eventtype.equals("NE_FLOW")) {
        //南基自定义规则判断，如是南基的自定义规则，则事件结果表的是否内置值为内置
        matchEvent += "~" + "0" + "~" + matchlogs(3).toString
      } else {
        matchEvent += "~" + "1" + "~" + matchlogs(3).toString
      }
      matchEvent += "#####" + matchlog

    }
    matchEvent
  }

  /**
    * 当事件name没有的时候,从范式化后的日志中获取规则相关信息,保存到redis中并返回结果
    *
    * @param matchlog
    * @param eventDefRule
    * @param jedis
    * @return
    */
  def dealEventRule(matchlog: String, eventDefRule: JSONArray,newEventDefId:String,jedis: Jedis): String = {
    var matchEvent = ""
    if(matchlog != null && !matchlog.trim.eq("") && matchlog.contains("~")){
      val matchlogs = matchlog.split("~")

      //保存到redis
      saveEventRuleToRedis(newEventDefId,matchlogs,eventDefRule,jedis)
      //新结果集id,这个id对应的数据发送到kafka与保存到hbase或ES中
      val newEventRuleResultId = UUID.randomUUID().toString.replaceAll("-","")
      val eventDefName = matchlogs(13)
      val eventRuleLevel = matchlogs(14)
      matchEvent = getEventRuleResult(newEventRuleResultId,newEventDefId,eventDefName,eventRuleLevel,matchlog)
    }
    matchEvent
  }


  /**
    * 单事件匹配--内置
    *
    * @param matchlog 范式化日志
    * @param eventDefRule 事件定义信息
    * @return
    */
  def eventMatchBySystem(matchlog:String,eventDefRule:JSONArray):String = {
    var matchEvent:String = ""
    if(matchlog != null && !matchlog.trim.eq("") && matchlog.contains("~")){
     // println("eventMatchBySystem==="+matchlog)
      val matchlogs = matchlog.split("~")
      val eventdefid = matchlogs(12).toString
      val eventdefname = matchlogs(13).toString
      val eventtype = matchlogs(3).toString

      var isFlag = true //标识从日志中来的规则在现有的规则库中是否已经存在
      for(i<- 0 to eventDefRule.size() -1){
        val eventDef:JSONObject = eventDefRule.get(i).asInstanceOf[JSONObject]
        val eventDefId = eventDef.getString("event_rule_id")     //事件定义ID
        val eventDefName = eventDef.getString("event_rule_name")   //事件名称
        val eventRuleLevel = eventDef.getString("event_rule_level")   //事件级别
        if(eventdefname.trim.equalsIgnoreCase(eventDefName.trim)){
          val eventRuleResultId = UUID.randomUUID().toString.replaceAll("-","")
          matchEvent = getEventRuleResult(eventRuleResultId,eventDefId,eventDefName,eventRuleLevel,matchlog)
          isFlag = false
          return matchEvent
        }
      }
    }
    matchEvent
  }

  /**
    * 单事件匹配--内置(以事件ID来匹配）
    *
    * @param matchlog 范式化日志
    * @return
    */
  def eventMatchBySystemForId(matchlog:String):String = {
    var matchEvent:String = ""
    if(matchlog != null && !matchlog.trim.eq("") && matchlog.contains("~")){
      val matchlogs = matchlog.split("~")
      val eventdefid = matchlogs(12).toString
      val eventdefname = matchlogs(13).toString
      val eventlevel = matchlogs(14).toString
      val eventtype = matchlogs(3).toString
      val eventid = matchlogs(21).toString

      val eventRuleResultId = UUID.randomUUID().toString.replaceAll("-","")
      matchEvent = eventRuleResultId + "~" + eventRuleResultId  + "~" + eventid  + "~" + matchlogs(0).toString + "~" + eventdefname
      matchEvent += "~" + eventlevel + "~" + matchlogs(4).toString + "~" + matchlogs(5).toString + "~" + matchlogs(6).toString
      if(eventtype != null && eventtype.equals("NE_FLOW")){ //南基自定义规则判断，如是南基的自定义规则，则事件结果表的是否内置值为内置
        matchEvent += "~" + matchlogs(7).toString + "~" + matchlogs(8).toString + "~" + matchlogs(2).toString + "~" + matchlogs(10).toString  + "~" + "0" + "~" + matchlogs(3).toString
      }else{
        matchEvent += "~" + matchlogs(7).toString + "~" + matchlogs(8).toString + "~" + matchlogs(2).toString + "~" + matchlogs(10).toString  + "~" + "1" + "~" + matchlogs(3).toString
      }
      matchEvent += "#####" + matchlog
    }
    matchEvent
  }

  def batchSaveSinalMatchEvent(events:List[String],stmt:Statement,client:Client):Unit = {
    if(events != null && events.size>0) {
      val tempevents: java.util.List[String] = new java.util.ArrayList()
      events.foreach(event => {
        if(event.toString.contains("#####")){
          val matchEventResult = event.toString.split("#####")(0).split("~")
          val rowkey = ROWUtils.genaralROW()
          var jsonEvent = new JSONObject()
          jsonEvent.put("ROW",rowkey)
          jsonEvent.put("EVENT_RULE_RESULT_ID",matchEventResult(1).toString)
          jsonEvent.put("EVENT_RULE_ID",matchEventResult(2).toString)
          jsonEvent.put("LOGID",matchEventResult(3).toString)
          jsonEvent.put("EVENT_RULE_NAME",matchEventResult(4).toString)
          jsonEvent.put("EVENT_RULE_LEVEL",matchEventResult(5).toString)
          jsonEvent.put("REPORTNEIP",matchEventResult(6).toString)
          jsonEvent.put("SOURCEIP",matchEventResult(7).toString)
          jsonEvent.put("SOURCEPORT",matchEventResult(8).toString)
          jsonEvent.put("DESTIP",matchEventResult(9).toString)
          jsonEvent.put("DESTPORT",matchEventResult(10).toString)
          jsonEvent.put("OPENTIME",DateUtils.dateToStamp(matchEventResult(11).toString))
          jsonEvent.put("ACTIONOPT",matchEventResult(12).toString)
          jsonEvent.put("IS_INNER",matchEventResult(13).toString)
          jsonEvent.put("PROTO",matchEventResult(14).toString)

          var keySet = jsonEvent.keys()
          var tempjson:JSONObject = new JSONObject();
          while (keySet.hasNext){
            var key:String = keySet.next().asInstanceOf[String];
            tempjson.put(key.toLowerCase(), jsonEvent.get(key));
          }
          jsonEvent = tempjson

          tempevents.add(jsonEvent.toString);
          IndexUtils.getBulkProcessor().add(new IndexRequest("event", "event").source(jsonEvent));

        }
      })
      val indexName = "event"
      val typeName = "event"
      println("event.size"+tempevents.size())
    }
  }
}
