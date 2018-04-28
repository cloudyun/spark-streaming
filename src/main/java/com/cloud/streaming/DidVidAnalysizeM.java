package com.cloud.streaming;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import com.cloud.vo.Message;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import scala.Tuple2;

public class DidVidAnalysizeM {

	private transient static Logger log = Logger.getLogger(DidVidAnalysizeM.class);

	private final static String dataName = "LookKafkaData";

	private final static String checkpoint = "/spark/checkpoint/" + dataName;

	private final static JavaStreamingManager sm = JavaStreamingManager.getInstance(dataName, checkpoint);

	private final static String TOPIC_CAPTURE = "captureTopic_test";

	private final static Set<String> topicCapture = new HashSet<String>(Arrays.asList(TOPIC_CAPTURE));

	// 创建一个 ZKGroupTopicDirs 对象，对保存
	private final static ZKGroupTopicDirs _CaptureTopicDirs = new ZKGroupTopicDirs("CAPTURE_TOPIC_DIR", TOPIC_CAPTURE);

	// 查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
	private final static int _CaptureChildren = sm.zkClient.countChildren(_CaptureTopicDirs.consumerOffsetDir());

	// 如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
	private static Map<TopicAndPartition, Long> _capturefromOffsets = new HashMap<TopicAndPartition, Long>();

	@SuppressWarnings("unused")
	private static JavaPairInputDStream<String, String> _captureKafkaStream = null;

	private static Map<String, String> _captureKafkaParams;

//	private static OffsetRange[] _macoffsetRanges;
	
	private static AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();

	static {
		_captureKafkaParams = new HashMap<String, String>();
		_captureKafkaParams.put("metadata.broker.list", sm.brokers);
		_captureKafkaParams.put("auto.offset.reset", "smallest");// largest
		_captureKafkaParams.put("group.id", "look-3");
		_captureKafkaParams.put("zookeeper.connect", sm.zkHost);
	}

	public static void main(String[] args) throws FileNotFoundException {
		
		log.info("统计开始!");

		// 从Kafka中获取数据转换成RDD
		JavaPairDStream<String, String> stream = createMacDirectStream();

		new DidVidAnalysize().analysize(stream);

		updateMacOffsetToZookeeper(stream);
		
		sm.run();

	}

	private static JavaPairDStream<String, String> createMacDirectStream() {
		if (_CaptureChildren <= 0) {
			return KafkaUtils.createDirectStream(sm.jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, _captureKafkaParams, topicCapture);
		}
		for (int x = 0; x < _CaptureChildren; x++) {
			String partitionOffset = sm.zkClient.readData(_CaptureTopicDirs.consumerOffsetDir() + "/" + x).toString();
			TopicAndPartition tp = new TopicAndPartition(TOPIC_CAPTURE, x);
			// 将不同 partition 对应的 offset 增加到 fromOffsets 中
			_capturefromOffsets.put(tp, Long.parseLong(partitionOffset));
		}

		Function<MessageAndMetadata<String, String>, Message> fc = new Function<MessageAndMetadata<String, String>, Message>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Message call(MessageAndMetadata<String, String> meta) throws Exception {
				return new Message(meta.topic(), meta.message());

			}

		};
		JavaInputDStream<Message> ds = KafkaUtils.createDirectStream(sm.jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, Message.class, _captureKafkaParams, _capturefromOffsets, fc);
		return ds.mapToPair(new PairFunction<Message, String, String>() {

			private static final long serialVersionUID = -4615930591412570078L;

			@Override
			public Tuple2<String, String> call(Message message) throws Exception {
				return new Tuple2<String, String>(message.topic(), message.message());
			}
		});
	}

	private static void updateMacOffsetToZookeeper(JavaPairDStream<String, String> stream) {
		
		stream.map(new Function<Tuple2<String,String>, String>() {

			private static final long serialVersionUID = 5460210750078682643L;

			@Override
			public String call(Tuple2<String, String> value) throws Exception {
				return value._2;
			}
		}).transform(new Function<JavaRDD<String>, JavaRDD<String>>() {

			private static final long serialVersionUID = 3671605639405616123L;

			@Override
			public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
				
				RDD<String> rdd2 = rdd.rdd();
				
				
				OffsetRange[] offsets = ((HasOffsetRanges) rdd2).offsetRanges();
				offsetRanges.set(offsets);
				return rdd;
			}
		}).foreachRDD(new VoidFunction<JavaRDD<String>>() {

			private static final long serialVersionUID = 497596501586761964L;

			@Override
			public void call(JavaRDD<String> rdd) throws Exception {
				OffsetRange[] offsets = offsetRanges.get();
				if (offsets == null) {
					return;
				}
				for (OffsetRange o : offsets) {
					String zkPath = _CaptureTopicDirs.consumerOffsetDir() + "/" + o.partition();
					ZkUtils.updatePersistentPath(sm.zkClient, zkPath, String.valueOf(o.fromOffset()));
//					ZkUtils.apply(sm.zkClient, false).updatePersistentPath(zkPath, String.valueOf(o.fromOffset()), ZooDefs.Ids.OPEN_ACL_UNSAFE);
//					new ZkUtils(sm.zkClient, null, false).updatePersistentPath(zkPath, String.valueOf(o.fromOffset()), ZooDefs.Ids.OPEN_ACL_UNSAFE);
				}
			}
		});
	}
}