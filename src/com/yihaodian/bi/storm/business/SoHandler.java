package com.yihaodian.bi.storm.business;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;


/**
 * 处理订单信息公共抽象类
 * 
 * 
 */

public abstract class SoHandler  {

	private static final Logger logger = LoggerFactory.getLogger(SoHandler.class);
	
	public class Return{
		public Map<String,String> map = new HashMap<String,String>();
		public String getValue(String key){
			return map.get(key);
		}
	}
	public abstract Object handleOrder(Tuple tuple, JumpMQOrderVo parentOrder, JumpMQOrderVo order);
	public abstract Object handleSoItem(Tuple tuple, JumpMQOrderVo parentOrder, JumpMQOrderVo order, JumpMQOrderItemVo soItem);

	//  So 粒度处理
	public Object  processOrder(Tuple input, JumpMQOrderVo order) {
		
		logger.info("Message successfully received: " + order.baseInfo());
		
		if (order.getChildOrderList()!=null && !order.getChildOrderList().isEmpty()) {
			for(JumpMQOrderVo childOrder: order.getChildOrderList())
			{
				logger.info("Order successfully received: " + childOrder.baseInfo());
				
				handleOrder(input, order, childOrder);
				
				logger.info("Order successfully processed: " + childOrder.baseInfo());
			}
		}else {
			logger.info("Order successfully received: " + order.baseInfo());
			
			handleOrder(input, null, order);
			
			logger.info("Order successfully processed: " + order.baseInfo());
		}
		
		return null;
	}
	
	// SoItem 粒度处理
	public Object processSoItem(Tuple input, JumpMQOrderVo order) {		
		logger.info("Message successfully received: " + order.baseInfo());
		
		if (order.getChildOrderList()!=null && !order.getChildOrderList().isEmpty()) {
			for(JumpMQOrderVo childOrder : order.getChildOrderList())
			{
				logger.info("Order successfully received: " + childOrder.baseInfo());
				
				List<JumpMQOrderItemVo> soItemList = childOrder.getSoItemList() ;
				
				for(JumpMQOrderItemVo soItem : soItemList)
				{
					if (soItem.getSubSoItemList() != null && !soItem.getSubSoItemList().isEmpty()) {
						
						List<JumpMQOrderItemVo> subSoItemList = soItem.getSubSoItemList();
						
						for (JumpMQOrderItemVo subSoItem : subSoItemList) {
							
							logger.info("SoItem successfully received: " + subSoItem.baseInfo());
							handleSoItem(input, order, childOrder, subSoItem);
							logger.info("SoItem successfully processed: " + subSoItem.baseInfo());
						}
					}
					else {
						logger.info("SoItem successfully received: " + soItem.baseInfo());
						handleSoItem(input, order, childOrder, soItem);
						logger.info("SoItem successfully processed: " + soItem.baseInfo());
					}
				}
				logger.info("Order successfully processed: " + childOrder.baseInfo());
			}
		}else {
			logger.info("Order successfully received: " + order.baseInfo());
			
			List<JumpMQOrderItemVo> soItemList = order.getSoItemList();
			
			for(JumpMQOrderItemVo soItem : soItemList)
			{
				if (soItem.getSubSoItemList() != null && !soItem.getSubSoItemList().isEmpty()) {
					
					List<JumpMQOrderItemVo> subSoItemList = soItem.getSubSoItemList();
					
					for (JumpMQOrderItemVo subSoItem : subSoItemList) {
						
						logger.info("SoItem successfully received: " + subSoItem.baseInfo());
						handleSoItem(input, null, order, subSoItem);
						logger.info("SoItem successfully processed: " + subSoItem.baseInfo());
					}
				}
				else {
					logger.info("SoItem successfully received: " + soItem.baseInfo());
					handleSoItem(input, null, order, soItem);
					logger.info("SoItem successfully processed: " + soItem.baseInfo());
				}
			}
			logger.info("Order successfully processed: " + order.baseInfo());
		}
		
		return null;
	}
}