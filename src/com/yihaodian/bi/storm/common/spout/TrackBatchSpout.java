package com.yihaodian.bi.storm.common.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.yihaodian.bi.storm.common.model.TrackerVo;
import com.yihaodian.bi.storm.common.mq.TrackerBatchConsumer;
import com.yihaodian.bi.storm.common.util.Constant;

public class TrackBatchSpout implements IRichSpout{

	/**
	 * Tracker 批处理spout 类
	 * 
	 */
	
	
	private static final long serialVersionUID = -633828471355602266L;
	private SpoutOutputCollector collector;
	private Queue<List<TrackerVo>> queue = null;
	private List<TrackerVo> list = null;
	
	Fields fields = new Fields(new ArrayList<String>());
	
	private String kafkaComsumerName ;
	public TrackBatchSpout(String kafkaComsumerName)
	{
		this.kafkaComsumerName = kafkaComsumerName;
		
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		if (! queue.isEmpty()) {
			list = queue.poll();
			System.out.println(Thread.currentThread().getName() +" TrackBatchSpout+++++++++++++++++++++++---list---"+list.get(0).getUrl()  );
			collector.emit(new Values(list));
			try {
//				Thread.sleep(2000);
			} catch (Exception e) {
				e.printStackTrace();
			}	
//			System.out.println("spout queue:"+queue.size());
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO 初始化
		System.out.println("TrackBatchSpout open方法执行........");
		this.collector = collector;
		
		TrackerBatchConsumer mq = new TrackerBatchConsumer(kafkaComsumerName);
		mq.start();
		queue = mq.getQueue() ;
		System.out.println("spout queue init:"+queue.size());
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("track"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

}
