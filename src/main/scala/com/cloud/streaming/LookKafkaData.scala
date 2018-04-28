package com.cloud.streaming

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.OffsetRange
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZKGroupTopicDirs
import kafka.utils.ZkUtils
import org.apache.log4j.Logger
import com.cloud.manager.StreamingManager

object LookKafkaData {
  
  @transient lazy val logg=Logger.getLogger(this.getClass)
  
  val dataName="LookKafkaData"
  
  val sm:StreamingManager= StreamingManager.getInstance(dataName)
  
  val TOPIC_CAPTURE ="captureTopic_test"
  
  val topicCapture = Set(TOPIC_CAPTURE)
  
  val _CaptureTopicDirs = new ZKGroupTopicDirs("CAPTURE_TOPIC_DIR", TOPIC_CAPTURE) //创建一个 ZKGroupTopicDirs 对象，对保存
  
  val _CaptureChildren = sm.zkClient.countChildren(s"${_CaptureTopicDirs.consumerOffsetDir}")     //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
  
  var _capturefromOffsets: Map[TopicAndPartition, Long] = Map()   //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
  
  var _captureKafkaStream: InputDStream[(String, String)] = null
  //smallest-largest
  val _captureKafkaParams = Map[String, String]("metadata.broker.list" -> sm.brokers, "auto.offset.reset"->"largest","group.id"->"look-2","zookeeper.connect"->sm.zkHost)
  
  val checkpoint ="/spark/checkpoint/"+dataName
  
  sm.ssc.checkpoint(checkpoint)
  
  var _macoffsetRanges = Array[OffsetRange]()
  
  def main(args: Array[String]): Unit = {
    
    logg.info("统计开始！")
    
		val _macKafkaStream:InputDStream[(String, String)]=createMacDirectStream()
    //处理mac的消息
    logg.info("mac消息处理！")
    new PrintData().printdata(_macKafkaStream)
    updateMacOffsetToZookeeper(_macKafkaStream)
    
    //启动任务
    
    sm.sscRun()
  }
  
  def createMacDirectStream():InputDStream[(String, String)]={
       if (_CaptureChildren > 0) { //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
          for (i <- 0 until _CaptureChildren) {
            
            val partitionOffset = sm.zkClient.readData[String](s"${_CaptureTopicDirs.consumerOffsetDir}/${i}")
            
            val tp = TopicAndPartition(TOPIC_CAPTURE, i)
            
            _capturefromOffsets += (tp -> partitionOffset.toLong) //将不同 partition 对应的 offset 增加到 fromOffsets 中
          }
    
          val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message()) //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
          
          _captureKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](sm.ssc, _captureKafkaParams, _capturefromOffsets, messageHandler)
          
        } else {
          
          _captureKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sm.ssc, _captureKafkaParams, topicCapture) //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
        }
          
        _captureKafkaStream
    }
  
  def updateMacOffsetToZookeeper(stream:InputDStream[(String, String)]):Unit={
    		stream.transform{rdd=>
        _macoffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
        rdd
        }.map(_._2).foreachRDD(rdd=>{
        	for (o <- _macoffsetRanges) {
        		val zkPath = s"${_CaptureTopicDirs.consumerOffsetDir}/${o.partition}"
        		ZkUtils.updatePersistentPath(sm.zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
        	}
        })
    }
}