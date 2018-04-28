package com.cloud.streaming;

import java.util.HashMap;
import java.util.Map;
import org.I0Itec.zkclient.ZkClient;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**  
 * @Title:  StreamingManager.java   
 * @Package com.fhzz.spark.wordcount   
 * @Description:    (用一句话描述该文件做什么)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年4月27日 下午3:19:27   
 * @version V1.0 
 */ 
public class JavaStreamingManager {
	
	private static Map<String, JavaStreamingManager> instances = null;
	
	public SparkConf sparkConf;
	
	public JavaStreamingContext jssc;
	

	public String brokers="yh-ambari01.lingda.com:6667,yh-ambari02.lingda.com:6667,yh-ambari03.lingda.com:6667";
	
	public String zkHost = "yh-ambari03.lingda.com:2181,yh-ambari01.lingda.com:2181,yh-ambari02.lingda.com:2181";
	
	public ZkClient zkClient = new ZkClient(zkHost);
	
	private JavaStreamingManager(String name, String checkpoint) {
		sparkConf = new SparkConf();
		sparkConf.setAppName("vidServer" + name);
		sparkConf.set("spark.streaming.unpersist", "true");
		sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "500");
		sparkConf.setMaster("local[3]");
		sparkConf.set("spark.default.parallelism", "150");
		
		jssc = new JavaStreamingContext(sparkConf, new Duration(10));
		jssc.checkpoint(checkpoint);
	}
	
	public static JavaStreamingManager getInstance(String name, String checkpoint) {
		if (instances == null) {
			instances = new HashMap<String, JavaStreamingManager>();
		}
		JavaStreamingManager instance = instances.get(name);
		if (instance == null) {
			instance = new JavaStreamingManager(name, checkpoint);
			instances.put(name, instance);
		}
		return instance;
	}
	
	public void run() {
		jssc.start();
		jssc.awaitTermination();
	}
}