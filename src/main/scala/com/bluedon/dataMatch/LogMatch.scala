package com.bluedon.dataMatch

import java.sql.{Connection, Statement}
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util
import java.util.UUID

import com.bluedon.utils._
import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql._
import org.apache.phoenix.spark._
import com.bluedon.esinterface.config.ESClient
import com.bluedon.esinterface.index.IndexUtils
import com.bluedon.esinterface.statics.ESStaticsUtils
import org.apache.commons.lang3.StringUtils
import org.elasticsearch.client.Client

import scala.util.control.Breaks._
import java.util.regex.{Matcher, Pattern}

import org.elasticsearch.action.index.IndexRequest

/**
  * Created by dengxiwen on 2016/12/27.
  */
class LogMatch extends  Serializable{

  /**
    * 根据原始日志匹配的字段获取对应的值
    *
    * @param matchList
    * @param fieldKey
    * @return
    */
  def getMatchValue(matchList: Matcher, fieldKey: String): String = {
    var resValue = ""

    if(StringUtils.isNotEmpty(fieldKey)){
      try{
        resValue=  matchList.group(fieldKey)
      }catch {
        case ex:Exception=>{
          resValue = ""
        }
      }
    }

    resValue
  }

  /**
    * 匹配日志-内置
    *
    * @param x
    * @param logRegex
    * @return
    */
  def matchLog(x:String,logRegex:JSONArray):String = {
    //val rules = logRegex.collect()
    var matchStr = ""
    val rules = logRegex
    val receiveLog = x.split("~")
    if(receiveLog != null && receiveLog.length>6){
      val logStr = receiveLog(6)
      var reportip = receiveLog(2)
      /*println("字符串="+x)
      println("规则条数"+rules.size()+"上报IP="+reportip)*/
      val IPP = logStr.split("\\|!")
      reportip=IPP(29)
      breakable {
        //根据上报设备IP过滤日志匹配规则
        for(i <- 0 to rules.size()-1) {
          val rule:JSONObject = rules.get(i).asInstanceOf[JSONObject]
          //println("rule="+rule)
          if (reportip != null && rule.get("device_ip") != null && reportip.trim.equals(rule.getString("device_ip").trim)) {
            val log_rule_id = rule.getString("log_rule_id").trim
            val matchRegexStr = rule.getString("log_regex").trim.r
            var matchLogStr:String = ""   //匹配的字符串
            //println("是否是南极数据|!"+logStr)
            if(logStr != null && logStr.contains("|!")) {//如果是南基数据
            val njlogs = logStr.split("\\|!")
              matchLogStr = njlogs(66).toString.trim
            }else{
              matchLogStr = logStr
            }
            matchLogStr = matchLogStr.trim

            var matchList = matchRegexStr.pattern.matcher(matchLogStr)

            //ROW,recordid,firstrecvtime,reportapp,reportip,sourceip,sourceport,destip,destport,eventaction,actionresult,reportnetype,eventdefid,eventname,eventlevel,orgid
            //规则匹配

            if (matchList != null && matchList.groupCount > 0 && matchList.find()) {
              // matchStr = dealOriginalLog(receiveLog,rule,matchList)

              val recordid = UUID.randomUUID().toString.replaceAll("-", "")
              matchStr = recordid + "~" + recordid + "~" + receiveLog(1) + "~" + receiveLog(5) + "~" + receiveLog(2)

              //源IP
              val sourceIp = rule.get("sourceip").toString.trim  //得到sourceip索引
              if (sourceIp != null && StringUtils.isNotEmpty(sourceIp)) {
                val sourceipIndex = sourceIp.toInt
                val sourceip =  matchList.group(sourceipIndex) //获取对应位置上sourceip
                if(StringUtils.isNotEmpty(sourceip)){
                  matchStr += "~" +sourceip
                }else {
                  matchStr += "~" + " "
                }
              } else {
                matchStr += "~" + " "
              }

              //源端口
              val sourcePort = rule.get("sourceport").toString.trim
              if (sourcePort != null && StringUtils.isNotEmpty(sourcePort)) {
                val sourcePortIndex = sourcePort.toInt
                val sourceport = matchList.group(sourcePortIndex)
                if(StringUtils.isNotEmpty(sourceport)){
                  matchStr += "~" + sourceport.toString.replace(":", "")
                }else{
                  matchStr += "~" + " "
                }
              } else {
                matchStr += "~" + " "
              }

              //目的IP
              val destIp = rule.get("destip").toString.trim
              if (destIp != null && StringUtils.isNotEmpty(destIp)) {
                val destIpIndex = destIp.toInt
                val destip = matchList.group(destIpIndex)
                if(StringUtils.isNotEmpty(destip)){
                  matchStr += "~" + destip.toString
                }else{
                  matchStr += "~" + " "
                }
              } else {
                matchStr += "~" + " "
              }

              //目的端口
              val destPort = rule.get("destport").toString.trim
              if (destPort != null && StringUtils.isNotEmpty(destPort)) {
                val destPortIndex = destPort.toInt
                val destport = matchList.group(destPortIndex)
                if(StringUtils.isNotEmpty(destport)){
                  matchStr += "~" + destport.toString.replace(":", "")
                }else{
                  matchStr += "~" + " "
                }
              } else {
                matchStr += "~" + " "
              }

              //执行操作
              val action = rule.get("eventaction").toString.trim
              if (action != null && StringUtils.isNotEmpty(action)) {
                val actionIndex = action.toInt
                val eventAction = matchList.group(actionIndex)
                if(StringUtils.isNotEmpty(eventAction)){
                  matchStr += "~" + eventAction.toString
                }else{
                  matchStr += "~" + " "
                }
              } else {
                matchStr += "~" + " "
              }

              //执行结果
              val actionres = rule.get("actionresult").toString.trim
              if ( actionres != null && StringUtils.isNotEmpty(actionres)) {
                val actionResIndex = actionres.toInt
                val actionresult = matchList.group(actionResIndex)
                if(StringUtils.isNotEmpty(actionresult)){
                  matchStr += "~" + actionresult.toString
                }else{
                  matchStr += "~" + " "
                }
              } else {
                matchStr += "~" + " "
              }

              //设备类型
              if (rule.get("reportnetype") != null) {
                val reportnetype = rule.getString("reportnetype")
                matchStr += "~" + reportnetype
              } else {
                matchStr += "~" + " "
              }

              //事件类型
              if (rule.get("eventdefid") != null) {
                val eventdefid = rule.getString("eventdefid").replaceAll("-", "")
                matchStr += "~" + eventdefid
              } else {
                matchStr += "~" + " "
              }

              //事件名称,这里eventname是随着log一起给出的
              val eventName = rule.get("eventname").toString.trim
              if ( eventName != null && StringUtils.isNotEmpty(eventName)) {
                val eventNameIndex = eventName.toInt
                val eventname = matchList.group(eventNameIndex)
                if(StringUtils.isNotEmpty(eventname)){
                  matchStr += "~" + eventname
                }else{
                  matchStr += "~" + " "
                }
              } else {
                matchStr += "~" + " "
              }

              //事件级别
              if (rule.get("eventlevel") != null) {
                val eventlevel = rule.getString("eventlevel")
                if(StringUtils.isNotEmpty(eventlevel)){
                  matchStr += "~" + eventlevel
                }else{
                  matchStr += "~1"
                }
              } else {
                matchStr += "~" + "1"
              }

              //原始日志
              matchStr += "~" + receiveLog(0)

              //应用层协议
              val appProto = rule.get("appproto").toString.trim
              if ( appProto != null && StringUtils.isNotEmpty(appProto)) {
                val appProtoIndex = appProto.toInt
                val appproto = matchList.group(appProtoIndex)
                if(StringUtils.isNotEmpty(appproto)){
                  matchStr += "~" + appproto.toString
                }else{
                  matchStr += "~" + " "
                }
              } else {
                matchStr += "~" + " "
              }

              //URL
              val urlStr = rule.get("url").toString.trim
              if ( urlStr != null && StringUtils.isNotEmpty(urlStr)) {
                val urlIndex = urlStr.toInt
                val url = matchList.group(urlIndex)
                if(StringUtils.isNotEmpty(url)){
                  matchStr += "~" + url.toString
                }else{
                  matchStr += "~" + " "
                }
              } else {
                matchStr += "~" + " "
              }

              //GET参数
              val getparam = rule.get("getparameter").toString.trim
              if (getparam != null  && StringUtils.isNotEmpty(getparam)) {
                val getparamIndex = getparam.toInt
                val getparameter = matchList.group(getparamIndex)
                if(StringUtils.isNotEmpty(getparameter)){
                  matchStr += "~" + getparameter.toString
                }else{
                  matchStr += "~" + " "
                }
              } else {
                matchStr += "~" + " "
              }

              //PROTO
              val protoStr = rule.get("proto").toString.trim
              if (protoStr != null  && StringUtils.isNotEmpty(protoStr)) {
                val protoIndex = protoStr.toInt
                val proto = matchList.group(protoIndex)
                if(StringUtils.isNotEmpty(proto)){
                  matchStr += "~" + proto.toString
                }else{
                  matchStr += "~" + " "
                }
              } else {
                matchStr += "~" + " "
              }

              //OPENTIME
              val openTime = rule.get("opentime").toString.trim
              if ( openTime != null && StringUtils.isNotEmpty(openTime)) {
                val openTimeIndex = openTime.toInt
                val opentime = matchList.group(openTimeIndex)
                if(StringUtils.isNotEmpty(opentime)){
                  matchStr += "~" + opentime.toString
                }else{
                  matchStr += "~" + " "
                }
              } else {
                matchStr += "~" + " "
              }

              //事件id
              if (rule.get("eventid") != null) {
                val eventid = rule.getString("eventid")
                matchStr += "~" + eventid
              } else {
                matchStr += "~" + " "
              }
              matchStr += "~" + matchLogStr + "~" + log_rule_id
              return matchStr.toString
              break
            }
          }
        }
      }
    }
    //不匹配返回空
    matchStr
  }

  /**
    * 匹配日志-自定义
    *
    * @param x
    * @param logRegex
    * @return
    */
  def matchLogUser(x:String,logRegex:JSONArray):String = {
    //val rules = logRegex.collect()
    var matchStr = ""
    val rules = logRegex
    val receiveLog = x.split("~")
    if(receiveLog != null && receiveLog.length>6){
      val logStr = receiveLog(6)
      val reportip = receiveLog(2)


      breakable {
        //根据上报设备IP过滤日志匹配规则

        for(i <- 0 to rules.size()-1) {
          val rule:JSONObject = rules.get(i).asInstanceOf[JSONObject]
          val log_rule_id = rule.getString("log_rule_id").trim
          val matchRegexStr = rule.getString("log_regex").trim.r
          val matchList = matchRegexStr.pattern.matcher(logStr)
          //ROW,recordid,firstrecvtime,reportapp,reportip,sourceip,sourceport,destip,destport,eventaction,actionresult,reportnetype,eventdefid,eventname,eventlevel,orgid
          //规则匹配

          if (matchList != null && matchList.groupCount > 0 && matchList.find()) {
            // matchStr = dealOriginalLog(receiveLog,rule,matchList)

            val recordid = UUID.randomUUID().toString.replaceAll("-", "")
            matchStr = recordid + "~" + recordid + "~" + DateUtils.dateToStamp(receiveLog(1)) + "~" + receiveLog(5) + "~" + receiveLog(2)

            //源IP
            val sourceip = getMatchValue(matchList,"sourceIp")
            if(StringUtils.isNotEmpty(sourceip)){
              matchStr += "~" +sourceip
            }else {
              matchStr += "~" + " "
            }
            /*val sourceIp = rule.get("sourceip").toString.trim  //得到sourceip索引
            if (sourceIp != null && StringUtils.isNotEmpty(sourceIp)) {
              val sourceipIndex = sourceIp.toInt
              val sourceip =  matchList.group(sourceipIndex) //获取对应位置上sourceip
              if(StringUtils.isNotEmpty(sourceip)){
                matchStr += "~" +sourceip
              }else {
                matchStr += "~" + " "
              }
            } else {
              matchStr += "~" + " "
            }*/

            //源端口
            //源端口
            val sourceport = getMatchValue(matchList,"sourcePort")
            if(StringUtils.isNotEmpty(sourceport)){
              matchStr += "~" + sourceport.toString.replace(":", "")
            }else{
              matchStr += "~" + " "
            }

            //目的IP
            val destip = getMatchValue(matchList,"destIp")
            if(StringUtils.isNotEmpty(destip)){
              matchStr += "~" + destip.toString
            }else{
              matchStr += "~" + " "
            }

            //目的端口
            val destport = getMatchValue(matchList,"destPort")
            if(StringUtils.isNotEmpty(destport)){
              matchStr += "~" + destport.toString.replace(":", "")
            }else{
              matchStr += "~" + " "
            }

            //执行操作
            val eventAction = getMatchValue(matchList,"actionOpt")
            if(StringUtils.isNotEmpty(eventAction)){
              matchStr += "~" + eventAction.toString
            }else{
              matchStr += "~" + " "
            }

            //执行结果
            val actionresult =getMatchValue(matchList,"actionResult")
            if(StringUtils.isNotEmpty(actionresult)){
              matchStr += "~" + actionresult.toString
            }else{
              matchStr += "~" + " "
            }

            //设备类型
            if (rule.get("reportnetype") != null) {
              val reportnetype = rule.getString("reportnetype")
              matchStr += "~" + reportnetype
            } else {
              matchStr += "~" + " "
            }

            //事件类型
            if (rule.get("eventdefid") != null) {
              val eventdefid = rule.getString("eventdefid").replaceAll("-", "")
              matchStr += "~" + eventdefid
            } else {
              matchStr += "~" + " "
            }

            //事件名称,这里eventname是随着log一起给出的
            val eventname = getMatchValue(matchList,"eventName")
            if(StringUtils.isNotEmpty(eventname)){
              matchStr += "~" + eventname
            }else{
              matchStr += "~" + " "
            }

            //事件级别
            if (rule.get("eventlevel") != null) {
              val eventlevel = rule.getString("eventlevel")
              if(StringUtils.isNotEmpty(eventlevel)){
                matchStr += "~" + eventlevel
              }else{
                matchStr += "~1"
              }
            } else {
              matchStr += "~" + "1"
            }

            //原始日志
            matchStr += "~" + receiveLog(0)

            //应用层协议
            val appproto = getMatchValue(matchList,"appProto")
            if(StringUtils.isNotEmpty(appproto)){
              matchStr += "~" + appproto.toString
            }else{
              matchStr += "~" + " "
            }

            //URL
            val url = getMatchValue(matchList,"url")
            if(StringUtils.isNotEmpty(url)){
              matchStr += "~" + url.toString
            }else{
              matchStr += "~" + " "
            }

            //GET参数
            val getparameter = getMatchValue(matchList,"getParameter")
            if(StringUtils.isNotEmpty(getparameter)){
              matchStr += "~" + getparameter.toString
            }else{
              matchStr += "~" + " "
            }

            //PROTO
            val proto = getMatchValue(matchList,"proto")
            if(StringUtils.isNotEmpty(proto)){
              matchStr += "~" + proto.toString
            }else{
              matchStr += "~" + " "
            }

            //OPENTIME
            val opentime = getMatchValue(matchList,"openTime")
            if(StringUtils.isNotEmpty(opentime)){
              matchStr += "~" + opentime.toString
            }else{
              matchStr += "~" + " "
            }

            //事件id
            if (rule.get("eventid") != null) {
              val eventid = rule.getString("eventid")
              matchStr += "~" + eventid
            } else {
              matchStr += "~" + " "
            }
            matchStr += "~" + logStr + "~" + log_rule_id
            return matchStr.toString
            break
          }

        }
      }
    }
    //不匹配返回空
    matchStr
  }

  /**
    * 批量原始日志入库
    *
    * @param logs
    * @param stmt
    */
  def   batchSaveSysLog(logs:List[String],stmt:Statement,client:Client):Unit = {
    if(logs != null && logs.size>0){
      val tempLogs:java.util.List[String] = new java.util.ArrayList()
      logs.foreach(log =>{
        if(log != null && log.contains("~") && log.toString.split("~").length>=8){
          val mlogs = log.toString.split("~")
          val rowkey = ROWUtils.genaralROW()
          var ismatch = 1
          if(mlogs(7).toString != null && !mlogs(7).toString.trim.equals("")){
            try{
              ismatch = Integer.parseInt(mlogs(7).toString.trim)
            }catch {
              case ex:Exception =>{
                ismatch = 0
              }
            }
          }
          var logJson:JSONObject = new JSONObject();
          logJson.put("ROW",rowkey)
          logJson.put("RECORDID",mlogs(0).toString)
          logJson.put("RECVTIME",DateUtils.dateToStamp(mlogs(1).toString.trim).toLong )
          logJson.put("STORAGETIME",DateUtils.dateToStamp(mlogs(1).toString.trim).toLong)
          logJson.put("REPORTIP",mlogs(2).toString)
          logJson.put("REPORTAPP",mlogs(5).toString)
          logJson.put("LOGCONTENT",mlogs(6).toString)
          logJson.put("ISMATCH",ismatch)

          var keySet = logJson.keys()
          var tempjson:JSONObject = new JSONObject();
          while (keySet.hasNext){
            var key:String = keySet.next().asInstanceOf[String];
            tempjson.put(key.toLowerCase(), logJson.get(key));
          }
          logJson = tempjson

          tempLogs.add(logJson.toString);
          IndexUtils.getBulkProcessor().add(new IndexRequest("syslog", "syslog").source(logJson));
        }
      })

      val indexName = "syslog"
      val typeName = "syslog"
      if(tempLogs.size()!=0){
        println("""=======================""")
        println("入库量："+tempLogs.size())
      }
    }
  }
  /**
    * 批量范式化日志入库
    *
    * @param logs
    * @param stmt
    */
  def batchSaveMatchLog(logs:List[String],stmt:Statement,client:Client):Unit = {

    if(logs != null && logs.size>0){
      val tempLogs:java.util.List[String] = new java.util.ArrayList()
      logs.foreach(log =>{
        if(log != null && log.contains("~") && log.toString.split("~").length>=15){
          val mlogs = log.toString.split("~")
          val rowkey = ROWUtils.genaralROW()
          var logJson:JSONObject = new JSONObject();
          logJson.put("ROW",rowkey)

          logJson.put("RECORDID",mlogs(1).toString)
          logJson.put("FIRSTRECVTIME",new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(mlogs(2).toString).getTime )
          logJson.put("REPORTAPP",mlogs(3).toString)
          logJson.put("REPORTIP",mlogs(4).toString)
          logJson.put("SOURCEIP",mlogs(5).toString)
          logJson.put("SOURCEPORT",mlogs(6).toString)
          logJson.put("DESTIP",mlogs(7).toString)
          logJson.put("DESTPORT",mlogs(8).toString)
          logJson.put("EVENTACTION",mlogs(9).toString)
          logJson.put("ACTIONRESULT",mlogs(10).toString)
          logJson.put("REPORTNETYPE",mlogs(11).toString)
          logJson.put("EVENTDEFID",mlogs(12).toString)
          logJson.put("EVENTNAME",mlogs(13).toString)
          if(!mlogs(13).toString.trim.equals("")){
            logJson.put("EVENTNAME2",UnicodeUtils.string2Unicode(mlogs(13).toString.trim).replace("\\u","socos"))
            logJson.put("EVENTNAME3",MD5Tools.MD5(mlogs(13).toString.trim))
          }else{
            logJson.put("EVENTNAME2","")
            logJson.put("EVENTNAME3","")
          }
          logJson.put("EVENTLEVEL",mlogs(14).toString)
          logJson.put("ORGID",mlogs(15).toString)
          logJson.put("APPPROTO",mlogs(16).toString)
          logJson.put("URL",mlogs(17).toString)
          logJson.put("GETPARAMETER",mlogs(18).toString)
          logJson.put("ORGLOG",mlogs(22).toString)
          logJson.put("LOGRULEID",mlogs(23).toString)

          var keySet = logJson.keys()
          var tempjson:JSONObject = new JSONObject();
          while (keySet.hasNext){
            var key:String = keySet.next().asInstanceOf[String];
            tempjson.put(key.toLowerCase(), logJson.get(key));
          }
          logJson = tempjson
          //println("logJson==="+logJson.toString)
          IndexUtils.getBulkProcessor().add(new IndexRequest("genlog", "genlog").source(logJson));
          tempLogs.add(logJson.toString);
        }
      })

      val indexName = "genlog"
      val typeName = "genlog"
      println("list.size==="+tempLogs.size())
      //IndexUtils.batchIndexData(client,indexName,typeName,tempLogs)
    }
  }



}
