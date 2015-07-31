package com.yihaodian.bi.storm.common.spout;

import java.util.Map;
import java.util.Queue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.mq.Gos_create_so;
import com.yihaodian.bi.storm.common.mq.Gos_update_cancel;
import com.yihaodian.bi.storm.common.mq.Gos_update_payment;

public class OrderSourceAndCancelSpout implements IRichSpout{

	/**
	 * 成交订单数据源Spout类
	 * 
	 *
	 */
	private static final long serialVersionUID = 7636567314813361706L;
	private SpoutOutputCollector collector;
	
	private static Queue<JumpMQOrderVo> createSoQueue = null;// static类型 ？  可以是普通类型，static 在此无意义
	private static Queue<JumpMQOrderVo> payUpdateSoQueue = null;
	private static Queue<JumpMQOrderVo> gosUpdateCancelQueue = null;
	
	private String consumerName = null;
	public OrderSourceAndCancelSpout(String consumerName)
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
		// TODO 消费queue
		if (! createSoQueue.isEmpty()) {
			this.collector.emit(new Values(createSoQueue.poll()));
			System.out.println("createSoQueue:"+createSoQueue.size());
		}
		if (! payUpdateSoQueue.isEmpty()) {
			this.collector.emit(new Values(payUpdateSoQueue.poll()));
			System.out.println("payUpdateSoQueue:"+payUpdateSoQueue.size());
		}
		
		if (! gosUpdateCancelQueue.isEmpty()) {
			this.collector.emit(new Values(gosUpdateCancelQueue.poll()));
			System.out.println("gosUpdateCancelQueue:"+gosUpdateCancelQueue.size());
		}
//		try {
//			Thread.sleep(2000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO 初始化操作
		this.collector = collector;
		
		Gos_create_so gosCreateSo = new Gos_create_so(consumerName);
		createSoQueue = gosCreateSo.getQueue();
		
		Gos_update_payment gosUpdatePayment = new Gos_update_payment(consumerName);
		payUpdateSoQueue = gosUpdatePayment.getQueue() ;
		
		Gos_update_cancel gosCancel = new Gos_update_cancel(consumerName);
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
