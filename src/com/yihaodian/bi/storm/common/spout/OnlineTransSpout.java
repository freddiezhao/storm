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

import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.mq.Gos_create_so;
import com.yihaodian.bi.storm.common.mq.Gos_update_payment;
import com.yihaodian.bi.storm.common.mq.JumperMQFilter;
import com.yihaodian.bi.storm.common.util.BusinessLogic;
import com.yihaodian.bi.storm.common.util.Constant;

/**
 * payment update spout
 *
 */
public class OnlineTransSpout implements IRichSpout{
	
	private static final long serialVersionUID = 7234266179790645302L;
	private final Logger logger = LoggerFactory.getLogger(OnlineTransSpout.class);
	private SpoutOutputCollector collector;
	private Gos_update_payment updatePay;
	
	private String consumerName;
	
	public OnlineTransSpout(String consumerName)
	{
		this.consumerName = consumerName;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		System.setProperty("global.config.path", Constant.GLOBAL_CONFIG_PATH);
		this.collector = collector;
		// 注册gos_update_payment（支付）队列
	    this.updatePay = new Gos_update_payment(consumerName);
	}
	
	@Override
	public void nextTuple() {
		if (!updatePay.getQueue().isEmpty()) {
			try {
				JumpMQOrderVo o = updatePay.getQueue().poll();
				
				logger.info("Message prepareing ...: " + o.baseInfo());
				if (JumperMQFilter.isOnlinePay(o)) {
					o.setPaymentCategory(Constant.ONLINE_PAY);
					logger.info("Message ready: " + o.baseInfo());
					this.collector.emit(new Values(o), o.getId());
					logger.info("Message successfully sent: " + o.baseInfo());
				}
			} catch (IllegalArgumentException e){
				logger.warn("Message not sent. Caused by:" + e.getMessage());
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("so"));
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
		logger.warn("Message Failed: { order_id: " + msgId + "}");
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}

