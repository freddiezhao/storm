package com.yihaodian.bi.storm.common.spout;

import java.util.Date;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.mq.Gos_create_so;
import com.yihaodian.bi.storm.common.mq.Gos_update_payment;
import com.yihaodian.bi.storm.common.mq.JumperMQFilter;
import com.yihaodian.bi.storm.common.util.BusinessLogic;
import com.yihaodian.bi.storm.common.util.Constant;

/**
 * order transactions
 *
 */
public class TransactionSpout implements IRichSpout{
	
	private static final long serialVersionUID = -6178311031772712151L;
	private final Logger logger = LoggerFactory.getLogger(TransactionSpout.class);
	private SpoutOutputCollector collector;
	private Gos_create_so createSo;
	private Gos_update_payment updatePay;
	
	private String consumerName;
	private Date curDate;
	private Date msgDate;
	private Date ordrDate;
	
	public TransactionSpout(String consumerName)
	{
		this.consumerName = consumerName;
	}

	@Override
	public void nextTuple() {
		if (!createSo.getQueue().isEmpty()) {
			try {
				JumpMQOrderVo o = createSo.getQueue().poll();
			
				// 跨天
				msgDate = o.getMsgSendTime();
				if (msgDate.getDate() != curDate.getDate() && msgDate.after(curDate)) {
					curDate = msgDate;
				}
				
				if (JumperMQFilter.isOfflinePay(o) ) {
					o.setPaymentCategory(Constant.OFFLINE_PAY);
					ordrDate = BusinessLogic.orderDate(o);
					if (ordrDate == null) throw new IllegalArgumentException("Unknown Order Date: " + o.baseInfo());
					if ( ordrDate.getDate() == curDate.getDate()) {
						this.collector.emit(new Values(o), o.getId());
						logger.info("Message successfully sent: " + o.baseInfo());
					}
				}
				else if (JumperMQFilter.isBalancePay(o) 
						|| o.getOrderPaymentConfirmDate() != null) {
					o.setPaymentCategory(Constant.ONLINE_PAY);
					ordrDate = BusinessLogic.orderDate(o);
					if (ordrDate == null) throw new IllegalArgumentException("Unknown Order Date: " + o.baseInfo());
					if ( ordrDate.getDate() == curDate.getDate()) {
						this.collector.emit(new Values(o), o.getId());
						logger.info("Message successfully sent: " + o.baseInfo());
					}
				}
			} catch (IllegalArgumentException e){
				logger.warn("Message not sent. Caused by:" + e.getMessage());
			} 
		}
		if (!updatePay.getQueue().isEmpty()) {
			try {
				JumpMQOrderVo o = updatePay.getQueue().poll();
				
				// 跨天
				msgDate = o.getMsgSendTime();
				if (msgDate.getDate() != curDate.getDate() && msgDate.after(curDate)) {
					curDate = msgDate;
				}
				
				if (JumperMQFilter.isOnlinePay(o)) {
					o.setPaymentCategory(Constant.ONLINE_PAY);
					ordrDate = BusinessLogic.orderDate(o);
					if (ordrDate == null) throw new IllegalArgumentException("Unknown Order Date: " + o.baseInfo());
					if (ordrDate.getDate() == curDate.getDate()) {
						this.collector.emit(new Values(o), o.getId());
						logger.info("Message successfully sent: " + o.baseInfo());
					}
				}
			} catch (IllegalArgumentException e){
				logger.warn("Message not sent. Caused by:" + e.getMessage());
			}
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		System.setProperty("global.config.path", Constant.GLOBAL_CONFIG_PATH);
		this.collector = collector;
		this.curDate = new Date();
		// 注册gos_create_so（下单）队列
		this.createSo = new Gos_create_so(consumerName);
		// 注册gos_update_payment（支付）队列
	    this.updatePay = new Gos_update_payment(consumerName);
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

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		logger.warn("Message Failed: { order_id: " + msgId + "}");
	}
}
