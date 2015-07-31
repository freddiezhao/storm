package com.yihaodian.bi.storm.common.mq;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.architecture.jumper.common.consumer.ConsumerType;
import com.yihaodian.architecture.jumper.common.consumer.MessageFilter;
import com.yihaodian.architecture.jumper.common.message.Destination;
import com.yihaodian.architecture.jumper.common.message.Message;
import com.yihaodian.architecture.jumper.consumer.BackoutMessageException;
import com.yihaodian.architecture.jumper.consumer.Consumer;
import com.yihaodian.architecture.jumper.consumer.ConsumerConfig;
import com.yihaodian.architecture.jumper.consumer.ConsumerFactory;
import com.yihaodian.architecture.jumper.consumer.MessageListener;
import com.yihaodian.architecture.jumper.consumer.impl.ConsumerFactoryImpl;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.configcentre.client.utils.YccGlobalPropertyConfigurer;

/**
 *  订单创建消息消费，只要货到付款的
 * @author lvpeng
 *
 */
public class Gos_create_so implements MessageListener{
	
	static final Logger logger = LoggerFactory.getLogger(Gos_create_so.class);

    private Consumer            consumer;
    private static int                 threadPoolSize = 10;
    private static String              topic = "gos_create_so";
    private static ConcurrentLinkedQueue<JumpMQOrderVo> queue = new ConcurrentLinkedQueue<JumpMQOrderVo>();
    private JumpMQOrderVo jumpMQOrderVo = null;
    
    public Gos_create_so(String consumerName) {
        ConsumerFactory cf = ConsumerFactoryImpl.getInstance();
        ConsumerConfig config = new ConsumerConfig();
        config.setConsumerType(ConsumerType.CLIENT_ACKNOWLEDGE);
        //config.setThreadPoolSize(threadPoolSize);
        Destination dest = Destination.topic(topic);

        consumer = cf.createConsumer(dest, consumerName, config);
        consumer.setListener(this);
        consumer.start();
    }
    
    
	@Override
	public void onMessage(Message message) throws BackoutMessageException{
		jumpMQOrderVo = message.transferContentToBean(JumpMQOrderVo.class);
		
		if (jumpMQOrderVo != null) {
				logger.info("From `gos_create_so` MQ received:" + jumpMQOrderVo.baseInfo());
				queue.add(jumpMQOrderVo);
				logger.info("From `gos_create_so` MQ successfully put:" + jumpMQOrderVo.baseInfo());
		}
		else {
			logger.info("In `gos_create_so` MQ, `jumpMQOrderVo` is NULL.");
		}
	}
	
	public ConcurrentLinkedQueue<JumpMQOrderVo> getQueue()
	{
		return queue;
	}
	
	public static void main(String[] args) {
	}
	
}
