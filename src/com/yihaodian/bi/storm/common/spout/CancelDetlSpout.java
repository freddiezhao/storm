package com.yihaodian.bi.storm.common.spout;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.mq.Gos_update_cancel;
import com.yihaodian.bi.storm.common.mq.JumperMQFilter;
import com.yihaodian.bi.storm.common.util.Constant;

/**
 * order transactions
 *
 */
public class CancelDetlSpout implements IRichSpout{
	
	private static final long serialVersionUID = 1121792351378592352L;
	private final Logger logger = LoggerFactory.getLogger(CancelDetlSpout.class);
	private SpoutOutputCollector collector;
	private Gos_update_cancel updateCancel;
	
	private String consumerName;
	
	public CancelDetlSpout(String consumerName)
	{
		this.consumerName = consumerName;
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
		
		if (!updateCancel.getQueue().isEmpty()) {
			try {
				JumpMQOrderVo o = updateCancel.getQueue().poll();
				
				if (JumperMQFilter.isOnlinePay(o)) {
					o.setPaymentCategory(Constant.ONLINE_PAY);
				}
				else if(JumperMQFilter.isOfflinePay(o)) {
					o.setPaymentCategory(Constant.OFFLINE_PAY);
				}
				else {
					throw new IllegalArgumentException("Neither ONLINE nor OFFLINE. Unknown Payment Category.");
				}
					
				this.collector.emit(new Values(o), o.getId());
				logger.info("Message successfully sent: " + o.baseInfo());
			} catch (IllegalArgumentException e) {
				logger.warn("Message not sent. Caused by:" + e.getMessage());
			}
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		System.setProperty("global.config.path", Constant.GLOBAL_CONFIG_PATH);
		this.collector = collector;
		// 注册gos_update_cancel（下单）队列
		this.updateCancel = new Gos_update_cancel(consumerName);

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
