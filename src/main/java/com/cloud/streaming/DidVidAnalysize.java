package com.cloud.streaming;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import scala.Tuple2;

/**  
 * @Title:  DidVidAnalysize.java   
 * @Package com.fhzz.spark.wordcount   
 * @Description:    TODO(用一句话描述该文件做什么)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年4月27日 下午2:31:56   
 * @version V1.0 
 */ 
public class DidVidAnalysize implements Serializable {

	private static final long serialVersionUID = 1655468747511102222L;

	private transient static Logger log = Logger.getLogger(DidVidAnalysize.class);

	private static final SimpleDateFormat format = new SimpleDateFormat("MMdd");
	
	private static final String SPLIT = "\t";
	
	public void analysize(JavaPairDStream<String, String> stream) {
		JavaDStream<String> map = stream.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = 1L;

			public Iterable<String> call(Tuple2<String, String> value) throws Exception {
				return translate(value._2);
			}
		});

		JavaPairDStream<String, Integer> pairs = map.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String value) {
				return new Tuple2<String, Integer>(value, 1);
			}

		});

		JavaPairDStream<String, Integer> reduce = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a + b;
			}
		});

		reduce.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
						while (iterator.hasNext()) {
							Tuple2<String, Integer> next = iterator.next();
							String key = "did" + next._1;
							Integer num = next._2;
							log.info("--------------" + key + ":" + num.toString());
						}
					}
				});
			}

		});
	}

	private List<String> translate(String json) {
		List<String> list = new ArrayList<String>();
		JSONObject data = null;
		try {
			data = JSON.parseObject(json);
		} catch (Exception e) {
			log.error("error json format! msg : " + json);
		}
		if (data == null) {
			return list;
		}

		String did = data.getOrDefault("cameraId", "").toString();
		String time = data.getOrDefault("captureTime", "0").toString();

		JSONArray faces = data.getJSONArray("captureFaces");
		if (faces == null) {
			return list;
		}
		String mmdd = format.format(new Date(Long.parseLong(time) * 1000));
		for (int x = 0; x < faces.size(); x++) {
			JSONArray vids = faces.getJSONObject(x).getJSONArray("vids");
			if (vids == null) {
				continue;
			}
			for (int y = 0; y < vids.size(); y++) {
				list.add(vids.getString(y) + SPLIT + did + SPLIT + mmdd);
			}
		}
		return list;
	}
}