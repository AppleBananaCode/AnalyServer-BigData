package com.bluedon.dataMatch
import java.util
import java.util.regex.Pattern
import java.util.{Properties, UUID}

import com.bluedon.esinterface.config.ESClient
import com.bluedon.utils._
import net.sf.json.{JSONArray, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
/**
  * Created by dengxiwen on 2017/5/10.
  */
class DataProcess {
  /**
    * 原始日志处理(内置规则)，处理后把范式化日志转发到kafka的genlog-topic通道
    *
    * @param spark
    * @param properties
    */
  def logProcess(spark:SparkSession,ssc:StreamingContext,properties:Properties,bcVarUtil:BCVarUtil): Unit ={

    val ruleVarBC = bcVarUtil.getBCMap()
    var ruleBC = spark.sparkContext.broadcast(ruleVarBC("ruleBC"))

    val url:String = properties.getProperty("aplication.sql.url")
    val username:String = properties.getProperty("aplication.sql.username")
    val password:String = properties.getProperty("aplication.sql.password")
    val redisHost:String = properties.getProperty("redis.host")
    val redisHostBD = spark.sparkContext.broadcast(redisHost)

    val zkQuorumRoot = properties.getProperty("zookeeper.host")+":" + properties.getProperty("zookeeper.port")
    val zkQuorumURL = spark.sparkContext.broadcast(zkQuorumRoot)
    val zkQuorumKafka = properties.getProperty("zookeeper.kafka.host")+":" + properties.getProperty("zookeeper.port")

    val kafkalist = properties.getProperty("kafka.host.list")
    val kafkalistURL = spark.sparkContext.broadcast(kafkalist)

    //获取kafka日志流数据

    //val syslogsCache = KafkaUtils.createStream(ssc, zkQuorumKafka, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER_2)
    val brokers = properties.getProperty("kafka.host.list")

    val topics = Set(properties.getProperty("kafka.log.topic")).toSet

    val kafkaParams = Map[String, String](
      "metadata.broker.list"        -> brokers,
      "bootstrap.servers"           -> brokers,
      "group.id"                    -> "sysloggp",
      "auto.commit.interval.ms"     -> "1000",
      "key.deserializer"            -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"          -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset"           -> "latest",
      "enable.auto.commit"          -> "true"
    )
    print(s"================ Load config finish ================= ")

    val syslogsCache: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String]( ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams) )
    print(s"================ get from kafka finish ================= ")
    /*
    //获取规则
    val jedis:Jedis = new Jedis(redisHost,6379)
    jedis.auth("123456");
    val ruleUtils = new RuleUtils
    var allRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAllRuleMapByRedis(jedis,url,username,password))
    var dataCheckMapBC = spark.sparkContext.broadcast(ruleUtils.getCheckDataMapByRedis(jedis,url,username,password))
    //获取告警规则
    var alarmRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAlarmRuleMapByRedis(jedis,url,username,password))
    jedis.close()
    */
    //日志范式化匹配与存储
      println("开始接受数据的时间"+new java.util.Date().getTime)
     syslogsCache.foreachRDD(syslogsIt=>{
       val a: RDD[ConsumerRecord[String, String]] = syslogsIt
       println("获取数量："+a.count())
      val allRuleMap = ruleBC.value.value("allRuleMapBC")
      val dataCheckMap = ruleBC.value.value("dataCheckMapBC")
      val alarmRuleMap = ruleBC.value.value("alarmRuleMapBC")

      var syslogLists = List[String]()
      var genlogLists = List[String]()
      var eventLists = List[String]()
      var eventdataList = new util.ArrayList[String]()
      val matchLog:LogMatch = new LogMatch
     // val relation:RelationEventMatch = new RelationEventMatch
      val logRule: JSONArray = allRuleMap("logRule")

      var rule: List[JSONObject] = List()
      for (i <-0 to  logRule.size()-1) {
        rule = rule :+ logRule.get(i).asInstanceOf[JSONObject]
      }

      val genrules: List[Pattern] = rule.map(x => {
        Pattern.compile(x.getString("log_regex").trim)
      })

      //获取内置事件规则
      val eventMatch = new EventMatch
      val eventDefRuleBySystem:JSONArray = allRuleMap("eventDefRuleBySystem")
      //数据库连接

      try{

        syslogsIt.foreachPartition(log=>{
          var client = ESClient.esClient()
          val b = log

          while (b.hasNext) {
            val log = b.next()

            var syslogs = log.value()

            if(syslogs != null && !syslogs.trim.equals("") && syslogs.trim.split("~")!=null && syslogs.trim.split("~").length<=6){
              val syslogsId = UUID.randomUUID().toString().replaceAll("-", "")
              syslogs = syslogsId + "~" + syslogs
            }
            val mlogs: String = matchLog.matchLog(syslogs,logRule)
            if(mlogs != null && !mlogs.trim.equals("")){
              genlogLists = genlogLists.::(mlogs)
              //  dbUtils.sendKafkaList("genlog-topic",mlogs,producer)
              syslogs = syslogs + "~0"  //原日志匹配
            }else{
              syslogs = syslogs + "~1"  //原日志不匹配
            }
            //            syslogs = syslogs + "~2"
            syslogLists = syslogLists.::(syslogs)

            val event = eventMatch.eventMatchBySystem(mlogs,eventDefRuleBySystem)
            eventLists = eventLists.::(event)

          }
          if(syslogLists.size>0){
            //保存原始日志
            matchLog.batchSaveSysLog(syslogLists,null,client)

            if(genlogLists.size>0){
              //保存范式化日志
              matchLog.batchSaveMatchLog(genlogLists,null,client)
            }
            if(eventLists.size>0){
              //保存内置事件
              eventMatch.batchSaveSinalMatchEvent(eventLists,null,client)

            }

          }
      })

      }catch {
        case e:Exception=>{
          println("#####phoenix发生异常!")
          e.printStackTrace()
        }
        case e:Exception=>{
          e.printStackTrace()
        }
      }finally {

      }

    })

  }


}
