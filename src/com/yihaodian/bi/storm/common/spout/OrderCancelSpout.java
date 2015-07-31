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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.mq.Gos_create_so;
import com.yihaodian.bi.storm.common.mq.Gos_update_cancel;
import com.yihaodian.bi.storm.common.mq.Gos_update_payment;
import com.yihaodian.bi.storm.common.mq.JumperMQFilter;
import com.yihaodian.bi.storm.common.util.BusinessLogic;
import com.yihaodian.bi.storm.common.util.Constant;

/**
 * order transactions
 *
 */
public class OrderCancelSpout implements IRichSpout{
	
	private static final long serialVersionUID = 1121792351378592352L;
	private final Logger logger = LoggerFactory.getLogger(OrderCancelSpout.class);
	private SpoutOutputCollector collector;
	private Gos_update_cancel updateCancel;
	
	private String consumerName;
	private Date curDate;
	private Date msgDate;
	private Date ordrDate;
	private Date cancelDate;
	
	public OrderCancelSpout(String consumerName)
	{
		this.consumerName = consumerName;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		System.setProperty("global.config.path", Constant.GLOBAL_CONFIG_PATH);
		this.collector = collector;
		curDate = new Date();
		// 注册gos_update_cancel（下单）队列
		this.updateCancel = new Gos_update_cancel(consumerName);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("so"));
	}
	
	@Override
	public void nextTuple() {
		
		if (!updateCancel.getQueue().isEmpty()) {
			try {
				JumpMQOrderVo o = updateCancel.getQueue().poll();
				
				// 判断跨天
				msgDate = o.getMsgSendTime();
				if (msgDate.getDate() != curDate.getDate() && msgDate.after(curDate)) {
					curDate = msgDate;
				}
				
				cancelDate = o.getCancelDate();
				if (cancelDate != null) {
					if (JumperMQFilter.isOnlinePay(o)) {
						o.setPaymentCategory(Constant.ONLINE_PAY);
					}
					else if(JumperMQFilter.isOfflinePay(o)) {
						o.setPaymentCategory(Constant.OFFLINE_PAY);
					}
					else {
						throw new IllegalArgumentException("Neither ONLINE nor OFFLINE. Unknown Payment Category.");
					}
					
					// 取今天的取消订单
					ordrDate = BusinessLogic.orderDate(o);
					if (ordrDate != null && ordrDate.getDate() == curDate.getDate() 
							&& ordrDate.getDate() == cancelDate.getDate()) {
						logger.info("Message ready: " + o.baseInfo());
						this.collector.emit(new Values(o), o.getId());
						logger.info("Message successfully sent: " + o.baseInfo());
					}
				}
				else {
					throw new IllegalArgumentException("Unkown Order cancel date.");
				}
			} catch (IllegalArgumentException e) {
				logger.warn("Message not taken. Caused by:" + e.getMessage());
			}
		}
	}
	
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void fail(Object msgId) {
		logger.warn("Message Failed: { order_id: " + msgId + "}");
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
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
