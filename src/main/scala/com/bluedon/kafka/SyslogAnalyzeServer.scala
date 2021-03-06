package com.bluedon.kafka
import java.util.UUID
import java.util.Date

import com.bluedon.listener.KafkaStreamListener
import com.bluedon.dataMatch._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import com.bluedon.initRule._
import com.bluedon.utils.{IpUtil, _}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql._
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import java.util.Properties
import java.io.InputStream
import java.sql.Connection

import net.sf.json.JSONArray
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.client.Client
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchStarted, StreamingListenerReceiverStarted}
import redis.clients.jedis.Jedis


/**
  * Created by Administrator on 2016/12/14.
  */
object SyslogAnalyzeServer {

  def main(args: Array[String]) {
    try{
      val properties:Properties = new Properties();
      val ipstream:InputStream=this.getClass().getResourceAsStream("/manage.properties");
      properties.load(ipstream);

      val url:String = properties.getProperty("aplication.sql.url")
      val username:String = properties.getProperty("aplication.sql.username")
      val password:String = properties.getProperty("aplication.sql.password")
      val redisHost:String = properties.getProperty("redis.host")

      val maxRatePerPartition:Int=properties.getProperty("syslog.maxRatePerPartition").toInt
      //是否开启最优 true
      val backpressure:String = properties.getProperty("syslog.backpressure")
      //消费窗口最大
      val times:Int =properties.getProperty("syslog.time").toInt

      val masterUrl = properties.getProperty("spark.master.url")
      val appName = properties.getProperty("spark.analyze.app.name")
      val spark = SparkSession
        .builder()
        .master(masterUrl)
        //.master("local")
        .appName(appName+"_syslog")
        .config("spark.some.config.option", "some-value")
        .config("spark.streaming.unpersist",true)  // 智能地去持久化
        .config("spark.streaming.stopGracefullyOnShutdown","true")  // 优雅的停止服务
        .config("spark.streaming.backpressure.enabled",backpressure)      //开启后spark自动根据系统负载选择最优消费速率
        .config("spark.streaming.backpressure.initialRate",15000)      //限制第一次批处理应该消费的数据，因为程序冷启动队列里面有大量积压，防止第一次全部读取，造成系统阻塞
        .config("spark.streaming.kafka.maxRatePerPartition",maxRatePerPartition)      //限制每秒每个消费线程读取每个kafka分区最大的数据量  8个分区总共1w
        .config("spark.streaming.receiver.maxRate",12000)      //设置每次接收的最大数量
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
      val sparkConf: SparkContext = spark.sparkContext

      //获取规则
      val jedis:Jedis = new Jedis(redisHost,6379)
      jedis.auth("123456");
      val ruleUtils = new RuleUtils
      var allRuleMapBC:Map[String, JSONArray] = ruleUtils.getAllRuleMapByRedis(jedis,url,username,password)

      jedis.close()

      val ruleMap = Map[String,Map[String, JSONArray]]("allRuleMapBC" -> allRuleMapBC)
      var ruleMapBC:Broadcast[Map[String, Map[String, JSONArray]]] = spark.sparkContext.broadcast(ruleMap)
      var bcVarUtil:BCVarUtil = new BCVarUtil()
      val ruleVarBC = scala.collection.mutable.Map[String,Broadcast[Map[String, Map[String, JSONArray]]]] ("ruleBC"->ruleMapBC)
      bcVarUtil.setBCMap(ruleVarBC)
      val time = new Date()
      bcVarUtil.setRefreshTime(time.getTime)

      //原始日志处理
      val ssc_syslog = new StreamingContext(sparkConf, Seconds(times))
      val kafkaStreamListener = new KafkaStreamListener()
      kafkaStreamListener.kafkaStreamListener(spark,bcVarUtil)
      //监听批处理
      ssc_syslog.addStreamingListener(kafkaStreamListener)

      //日志处理
      val dataProcess = new DataProcess
      dataProcess.logProcess(spark,ssc_syslog,properties,bcVarUtil)
      ssc_syslog.start()
      ssc_syslog.awaitTermination()
    }catch {
      case e:Exception=>{
        e.printStackTrace()
      }
    }

  }

}
