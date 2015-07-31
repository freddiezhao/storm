package com.yihaodian.bi.storm.common.mq;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.architecture.jumper.common.message.Destination;
import com.yihaodian.architecture.jumper.common.message.Message;
import com.yihaodian.architecture.jumper.consumer.BackoutMessageException;
import com.yihaodian.architecture.jumper.consumer.Consumer;
import com.yihaodian.architecture.jumper.consumer.ConsumerConfig;
import com.yihaodian.architecture.jumper.consumer.ConsumerFactory;
import com.yihaodian.architecture.jumper.consumer.MessageListener;
import com.yihaodian.architecture.jumper.consumer.impl.ConsumerFactoryImpl;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;

/**
 *  取消订单  Comsumer类
 *
 */
public class Gos_update_cancel implements MessageListener{

	static final Logger logger = LoggerFactory.getLogger(Gos_update_cancel.class);
    private Consumer            consumer;
    private static int                 threadPoolSize = 10;
    private static String              topic = "gos_update_cancel";
    
    public static ConcurrentLinkedQueue<JumpMQOrderVo> queue = new ConcurrentLinkedQueue<JumpMQOrderVo>();
    private JumpMQOrderVo jumpMQOrderVo = null;
    
    public Gos_update_cancel(String consumerName) {
        ConsumerFactory cf = ConsumerFactoryImpl.getInstance();
        ConsumerConfig config = new ConsumerConfig();
        //config.setThreadPoolSize(threadPoolSize);
        Destination dest = Destination.topic(topic);

        consumer = cf.createConsumer(dest, consumerName, config);
        consumer.setListener(this);
        consumer.start();
    }
    
    
	@Override
	public void onMessage(Message message) throws BackoutMessageException {

		jumpMQOrderVo = message.transferContentToBean(JumpMQOrderVo.class);
		
		if (jumpMQOrderVo != null) {
			logger.info("From `gos_update_cancel` MQ received: " + jumpMQOrderVo.baseInfo());
			queue.add(jumpMQOrderVo);
			logger.info("From `gos_update_cancel` MQ successfully put: " + jumpMQOrderVo.baseInfo());
		}
		else {
			logger.info("In `gos_update_cancel` MQ, `jumpMQOrderVo` is NULL.");
		}
	}
	
	public ConcurrentLinkedQueue<JumpMQOrderVo> getQueue()
	{
		return queue;
	}
	public static void main(String[] args) {
//		new Gos_update_cancel("bi_order_consumer");
	}
	
}
