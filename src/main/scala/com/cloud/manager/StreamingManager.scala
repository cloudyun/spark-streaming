package com.cloud.manager

import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.StreamingContext

class StreamingManager private (taskName: String) {
  //获取sparkstreaming
  val sparkConf = new SparkConf().setAppName("vidServer" + taskName)

  sparkConf.set("spark.streaming.unpersist", "true")

  //设置一个批次从kafka拉取的数据
  sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "50")

  sparkConf.setMaster("local[3]")

  sparkConf.set("spark.default.parallelism", "150")

  val ssc = new StreamingContext(sparkConf, Durations.seconds(10))

  val brokers = "yh-ambari01.lingda.com:6667,yh-ambari02.lingda.com:6667,yh-ambari03.lingda.com:6667"

  val zkHost = "yh-ambari03.lingda.com:2181,yh-ambari01.lingda.com:2181,yh-ambari02.lingda.com:2181"

  val zkClient = new ZkClient(zkHost)

  def sscRun() = {
    ssc.start()//开启
    ssc.awaitTermination()//等待
  }
}

object StreamingManager {

  private var streamingManager: StreamingManager = null

  def getInstance(taskName: String): StreamingManager = {
    if (streamingManager == null) {
      streamingManager = new StreamingManager(taskName);
    }
    streamingManager
  }
}