package com.yihaodian.bi.storm.common.mq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.storm.common.model.TrackerVo;
import com.yihaodian.bi.storm.common.util.Constant;
import com.yihaodian.bi.storm.common.util.URLUtils;

/**
 * Tracker数据kafka comsumer类，输出tracker  list 
 * 1、接收kafaka数据
 * 2、对tracker进行过滤
 * 
 * @author lvpeng
 *
 */
public class TrackerBatchConsumer extends Thread {
	
	private static final long serialVersionUID = 123L;
	
	private final ConsumerConnector consumer;
	private final String topic;
	private TrackerVo tracker = null;
	private Queue<List<TrackerVo>> queue = new ConcurrentLinkedQueue<List<TrackerVo>>();//线程安全下，不需要用static
	private static final Logger logger = LoggerFactory.getLogger(TrackerBatchConsumer.class);
	public static String kafkaComsumerName;
	public TrackerBatchConsumer(String kafkaComsumerName) {
		this.kafkaComsumerName = kafkaComsumerName;
		this.topic = Constant.KAFKA_CONSUMER_NAME_TRACKER;
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", Constant.KAFKA_ZK_CONNECTION);
		props.put("group.id", kafkaComsumerName);
//		props.put("group.id", "bi_test_comsumerName");
		props.put("zookeeper.session.timeout.ms", "10000");
		props.put("zookeeper.sync.time.ms", "2000");
		props.put("auto.commit.interval.ms", "10000");

		return new ConsumerConfig(props);

	}

	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		logger.info("consumerMap.size:" + consumerMap.size());

		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			//每个message里是100个tracker，且用\n 分割，所以先split
			String arr[] = new String(it.next().message()).split("\\n");
			List<TrackerVo> list = new ArrayList<TrackerVo>();
			for (String s : arr) 
			{
				String cols[] = s.split("\\t");
//				System.out.println("cols:"+cols.length);
				// 过滤一：列小于30的丢弃
				if (cols.length < 30) {
					continue ;
				}
				// 过滤二：过滤来自百度的爬虫数据
				if (s.contains("http://www.baidu.com/search/spider.html")) {
					continue ;
				}
				//过滤三：过滤URL
				if(!"".equals(cols[1])&&cols[1]!=null){//为空的不处理
					if (! "yhd.com".equals(URLUtils.getHostFromURL(cols[1]))) {
						continue ;
					}
				}
				if (cols[1].startsWith("http://union.yihaodian.com/link_make/viewPicInfo.do")
						|| cols[1].startsWith("http://union.yhd.com/resourceCenter/viewSearchBox.do")
						|| cols[1].startsWith("http://union.yhd.com/resourceCenter/viewRanking.do")
						|| cols[1].startsWith("http://union.yhd.com/resourceCenter/getUserCookies.do")
						|| cols[1].startsWith("http://union.yhd.com/resourceCenter/viewShoppingWindow.do")
						|| cols[1].startsWith("http://union.yhd.com/resourceCenter/getUserCookies.do")) {
					continue ;
				}
				tracker = new TrackerVo(s);
				// 过滤四：过滤button_position 不为空的
				if (tracker.getButton_position() != null 
						&& !"null".equals(tracker.getButton_position()) && tracker.getButton_position().length()>1) {
//					System.out.println("button:"+tracker.getButton_position());
					continue ;
				}
				list.add(tracker);
				System.out.println(arr.length+"------:"+cols.length+"--"+tracker.getUrl());
			}
			if (list.size()>0) {
				queue.add(list);
				System.out.println("kafka queue:" + queue.size()+"--"+list.size());
			}
			
		}

	}

	public Queue<List<TrackerVo>> getQueue() {
		logger.info(queue + "  :queue");
		return queue;
	}

	public static void main(String[] args) {
		TrackerBatchConsumer consumer = new TrackerBatchConsumer("bi_test_consumer");
		consumer.start();
	}
}
