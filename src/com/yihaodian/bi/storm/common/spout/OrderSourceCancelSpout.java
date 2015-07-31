package com.yihaodian.bi.storm.common.spout;

import java.util.Map;
import java.util.Queue;

import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.mq.Gos_update_cancel;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class OrderSourceCancelSpout implements IRichSpout{
	/**
	 * 取消订单数据源Spout类
	 *
	 */
	private SpoutOutputCollector collector;
	private static final long serialVersionUID = -6781670793359885927L;
	private static Queue<JumpMQOrderVo> gosUpdateCancelQueue = null;
	
	private String consumerName;
	private Gos_update_cancel gosCancel;

	
	public OrderSourceCancelSpout(String consumerName)
	{
		this.consumerName = consumerName ;
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

	@Override
	public void nextTuple() {
		
		if (! gosUpdateCancelQueue.isEmpty()) {
			this.collector.emit(new Values(gosUpdateCancelQueue.poll()));
			System.out.println("gosUpdateCancelQueue:"+gosUpdateCancelQueue.size());
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		System.setProperty("global.config.path", "/var/www/webapps/config");
		this.collector = collector;
		// 注册gos_update_cancel（取消）队列
	    gosCancel = new Gos_update_cancel(consumerName);
		gosUpdateCancelQueue = gosCancel.getQueue() ;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("so"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
