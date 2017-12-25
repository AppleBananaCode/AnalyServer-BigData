package com.bluedon.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._
/**
  * Created by huoguang on 2017/8/24.
  * 封装通过rdd， dataframe操作Elasticsearch
  * sparkSession config中需要写入es信息
  */
object ElasticsearchManager extends LogSupport{

  def saveDFtoES(df: DataFrame, indexAndtype: String, cfg: Map[String, String] = Map()) = {
    try {
      df.saveToEs(indexAndtype, cfg)
    } catch {
      case e:Throwable =>{
        log.error(s"Some Error Occur During Save DataFrame To Es : " + e.getMessage)
      }
    }
  }

  def getDFfromES(spark: SparkSession, indexAndtype: String, query: String) = {

    assert(indexAndtype != "" & query != "", " :Illegal input parameters !!")

    try {
      val df = spark.esDF(indexAndtype, query)
    } catch {
      case e: Exception => {
        log.error(s"Some Error Occur During Get DataFrame From Es : " + e.getMessage)
      }
    }
  }






}
