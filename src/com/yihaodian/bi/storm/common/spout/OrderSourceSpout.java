package com.yihaodian.bi.storm.common.spout;

import java.util.Map;
import java.util.Queue;

import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.mq.Gos_create_so;
import com.yihaodian.bi.storm.common.mq.Gos_update_payment;
import com.yihaodian.bi.storm.common.util.Constant;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class OrderSourceSpout implements IRichSpout{

	/**
	 * 成交订单数据源Spout类
	 *
	 */
	private static final long serialVersionUID = 7636567314813361706L;
	private SpoutOutputCollector collector;
	
	private static Queue<JumpMQOrderVo> createSoQueue = null;// static类型 ？
	private static Queue<JumpMQOrderVo> payUpdateSoQueue = null;// static类型 ？
	
	private String consumerName;
	private Gos_create_so gosCreateSo;
	private Gos_update_payment gosUpdatePayment;
	
	public OrderSourceSpout(String consumerName)
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
		if (!createSoQueue.isEmpty()) {
			JumpMQOrderVo jumpMQOrderVo = createSoQueue.poll();
			if (jumpMQOrderVo != null ) {
				if (jumpMQOrderVo.getPayServiceType() == 2 || jumpMQOrderVo.getPayServiceType() == 5 
						|| jumpMQOrderVo.getPayServiceType() == 9 || jumpMQOrderVo.getPayServiceType() == 8 
						|| jumpMQOrderVo.getPayServiceType() == 10 || jumpMQOrderVo.getPayServiceType() == 12) {
					jumpMQOrderVo.setPaymentCategory(Constant.OFFLINE_PAY);
					this.collector.emit(new Values(jumpMQOrderVo));
	
				}
				else if (jumpMQOrderVo.getPayServiceType() == 0 || jumpMQOrderVo.getOrderPaymentConfirmDate() != null) {
					jumpMQOrderVo.setPaymentCategory(Constant.ONLINE_PAY);
					this.collector.emit(new Values(jumpMQOrderVo));
				}
			}
		}
		if (!payUpdateSoQueue.isEmpty()) {
			JumpMQOrderVo jumpMQOrderVo = payUpdateSoQueue.poll();
			if (jumpMQOrderVo != null ) {
				if (jumpMQOrderVo.getPayServiceType() != 2 && jumpMQOrderVo.getPayServiceType() != 5 
						&& jumpMQOrderVo.getPayServiceType() != 9 && jumpMQOrderVo.getPayServiceType() != 8 
						&& jumpMQOrderVo.getPayServiceType() != 10 && jumpMQOrderVo.getPayServiceType() != 12) {
					jumpMQOrderVo.setPaymentCategory(Constant.ONLINE_PAY);
					this.collector.emit(new Values(jumpMQOrderVo));
				}
			}
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		System.setProperty("global.config.path", Constant.GLOBAL_CONFIG_PATH);
		this.collector = collector;
		// 注册gos_create_so（下单）队列
		gosCreateSo = new Gos_create_so(consumerName);
		createSoQueue = gosCreateSo.getQueue();
		// 注册gos_update_payment（支付）队列
		gosUpdatePayment = new Gos_update_payment(consumerName);
		payUpdateSoQueue = gosUpdatePayment.getQueue();
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
