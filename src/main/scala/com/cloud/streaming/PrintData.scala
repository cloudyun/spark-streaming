package com.cloud.streaming

import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.JSONArray
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.commons.lang.StringUtils
import java.util.Date
import org.apache.log4j.Logger

class PrintData extends java.io.Serializable with Logging {

  private val SPILT = "\t"

  @transient lazy val logger = Logger.getLogger(this.getClass)

  def printdata(stream: DStream[(String, String)]): Unit = {
    logInfo("查看kafka 中的数据")
    try { //按天统计
      logger.info("对vid和设备Id之间的关系进行记录：")
            val lines = stream.flatMap { sourceData => translate(sourceData._2) }.map { data => (data, 1) }.reduceByKey(_ + _)

//      val lines = stream.flatMap { sourceData =>
//        var viddids = new ArrayBuffer[String]
//        var sourceJsonData: JSONObject = null
//        try {
//          sourceJsonData = JSON.parseObject(sourceData._2)
//        } catch {
//          case t: Exception => logError("json error format! json : " + sourceData._2)
//        }
//        if (sourceJsonData != null) {
//          val cameraId = sourceJsonData.getOrDefault("cameraId", "").toString()
//          val captureTime = sourceJsonData.getOrDefault("captureTime", "0").toString()
//          val orgIds = sourceJsonData.getOrDefault("orgIds", "").toString()
//          var captureFaces: JSONArray = null
//          if (sourceJsonData.containsKey("captureFaces") && sourceJsonData.getJSONArray("captureFaces") != null) {
//            captureFaces = sourceJsonData.getJSONArray("captureFaces")
//          }
//          if (captureFaces != null) {
//            val mmdd = DateUtil.convertLong2StringMMdd(captureTime.toLong)
//            val size = captureFaces.size();
//            logInfo("face count : " + size)
//            for (a <- 0 to size - 1) {
//              val vidArr = captureFaces.getJSONObject(a).getJSONArray("vids");
//              if (vidArr != null) {
//                val vids = vidArr.toArray(); //json转为数组
//                for (vid <- vids) {
//                  //logger.info("从kafka接收消息对象,相机ID:" + cameraId + " 虚拟ID:" + vid)
//                  viddids += (vid + SPILT + cameraId + SPILT + orgIds + SPILT + mmdd)
//                }
//              } else {
//                logInfo("无vids" + captureFaces.getJSONObject(a))
//              }
//            }
//          }
//        }
//        Some(viddids)
//      }.flatMap(data => data).map { data => (data, 1) }.reduceByKey(_ + _)

      lines.foreachRDD(rdd =>
        rdd.foreachPartition {
          iterator =>
            while (iterator.hasNext) {
              val next = iterator.next()
              val key = "did"  + next._1
              val num = next._2
              logInfo("--------------" + key + ":" + num.toString())
            }
        })
    } catch {
      case t: Exception => logger.error("错误信息:" + t.getMessage)
    }
  }
  
  def main(args: Array[String]): Unit = {
    println(translate("123"))
  }

  def translate(json: String): Array[String] = {
    var viddids = new ArrayBuffer[String]
    var sourceJsonData: JSONObject = null
    try {
      sourceJsonData = JSON.parseObject(json)
    } catch {
      case t: Exception => logger.error("json error format! json : " + json)
    }
    if (sourceJsonData == null) {
      return viddids.toArray
    }
    val cameraId = sourceJsonData.getOrDefault("cameraId", "").toString()
    val captureTime = sourceJsonData.getOrDefault("captureTime", "0").toString()
    val orgIds = sourceJsonData.getOrDefault("orgIds", "").toString()
    var captureFaces: JSONArray = null
    if (sourceJsonData.containsKey("captureFaces") && sourceJsonData.getJSONArray("captureFaces") != null) {
      captureFaces = sourceJsonData.getJSONArray("captureFaces")
    }
    if (captureFaces == null) {
      return viddids.toArray
    }
    val mmdd = captureTime
    val size = captureFaces.size();
    logger.info("face count : " + size)
    for (a <- 0 to size - 1) {
      val vidArr = captureFaces.getJSONObject(a).getJSONArray("vids");
      if (vidArr != null) {
        val vids = vidArr.toArray(); //json转为数组
        for (vid <- vids) {
          viddids += (vid + SPILT + cameraId + SPILT + orgIds + SPILT + mmdd)
        }
      } else {
        logger.info("无vids" + captureFaces.getJSONObject(a))
      }
    }
    return viddids.toArray
  }
}