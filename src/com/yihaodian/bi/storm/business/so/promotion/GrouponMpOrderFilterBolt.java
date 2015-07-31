package com.yihaodian.bi.storm.business.so.promotion;

import java.util.Map;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Groupon and Mingping orders are filtered from source steam and flow to different
 * bolts respectively through this filter bolt. 
 * 
 *
 */
public class GrouponMpOrderFilterBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2964349550100550503L;

	private OutputCollector _collector;
	private static final int GRPON = 2;
	private static final int MP = 7;
	private static final Long UNKNOWN = -999999L;
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		JumpMQOrderVo jumpMQOrderVo = (JumpMQOrderVo)input.getValue(0);
		String orderDate = null;
		
		if (jumpMQOrderVo != null) {
			
			if(jumpMQOrderVo.getPayServiceType() != 2 
					&& jumpMQOrderVo.getPayServiceType() != 5 
					&& jumpMQOrderVo.getPayServiceType() != 8
					&& jumpMQOrderVo.getPayServiceType() != 9
					&& jumpMQOrderVo.getPayServiceType() != 10
					&& jumpMQOrderVo.getPayServiceType() != 12){//在线支付的取支付时间
				orderDate = DateUtil.getFmtDate(jumpMQOrderVo.getOrderPaymentConfirmDate(),DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
			}
			else  {//否则，取订单创建时间
				orderDate = DateUtil.getFmtDate(jumpMQOrderVo.getOrderCreateTime(),DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
			}
			
			if (orderDate != null) {
				if (jumpMQOrderVo.getChildOrderList() != null && !jumpMQOrderVo.getChildOrderList().isEmpty()) {
					Long parentOrderId = jumpMQOrderVo.getId();
					if (parentOrderId == null) parentOrderId = UNKNOWN;
					Long provId = jumpMQOrderVo.getGoodReceiverProvinceId();
					Long cityId = jumpMQOrderVo.getGoodReceiverCityId();
					Long buyTimes = jumpMQOrderVo.getBoughtTimes();
					for (JumpMQOrderVo childOrder : jumpMQOrderVo.getChildOrderList()) {
						
						Integer orderSource = childOrder.getOrderSource(); // orderSource == 3  mobile
						for (JumpMQOrderItemVo soItem : childOrder.getSoItemList()) {
							if (soItem.getSubSoItemList() != null && !soItem.getSubSoItemList().isEmpty()) {
								for (JumpMQOrderItemVo subSoItem: soItem.getSubSoItemList()) {

									if (soItem.getPromotionRuleType() == GRPON) {  // grpon_mb
										_collector.emit("grpon", input, new Values(orderDate, parentOrderId, 
												orderSource, provId, cityId, buyTimes, subSoItem));
									}
									else if (soItem.getPromotionRuleType() == MP) { //mp_mb
										_collector.emit("mp", input, new Values(orderDate, parentOrderId, 
												orderSource, provId, cityId, buyTimes, subSoItem));
									}
								}
							}
							else {
								if (soItem.getPromotionRuleType() == GRPON) {  // grpon_mb
									_collector.emit("grpon", input, new Values(orderDate, parentOrderId, 
											orderSource, provId, cityId, buyTimes, soItem));
								}
								else if (soItem.getPromotionRuleType() == MP) { //mp_mb
									_collector.emit("mp", input, new Values(orderDate, parentOrderId, 
											orderSource, provId, cityId, buyTimes, soItem));
								}
							}
						}
					}
				}
				else {
					Long parentOrderId = UNKNOWN;
					Integer orderSource = jumpMQOrderVo.getOrderSource(); // orderSource == 3  mobile
					Long provId = jumpMQOrderVo.getGoodReceiverProvinceId();
					Long cityId = jumpMQOrderVo.getGoodReceiverCityId();
					Long buyTimes = jumpMQOrderVo.getBoughtTimes();
					
					for (JumpMQOrderItemVo soItem : jumpMQOrderVo.getSoItemList()) {
						if (soItem.getSubSoItemList() != null && !soItem.getSubSoItemList().isEmpty()) {
							for (JumpMQOrderItemVo subSoItem: soItem.getSubSoItemList()) {

								if (soItem.getPromotionRuleType() == GRPON) {  // grpon_mb
									_collector.emit("grpon", input, new Values(orderDate, parentOrderId, 
											orderSource, provId, cityId, buyTimes, subSoItem));
								}
								else if (soItem.getPromotionRuleType() == MP) { //mp_mb
									_collector.emit("mp", input, new Values(orderDate, parentOrderId, 
											orderSource, provId, cityId, buyTimes, subSoItem));
								}
							}
						}
						else {
							if (soItem.getPromotionRuleType() == GRPON) {  // grpon_mb
								_collector.emit("grpon", input, new Values(orderDate, parentOrderId, 
										orderSource, provId, cityId, buyTimes, soItem));
							}
							else if (soItem.getPromotionRuleType() == MP) { //mp_mb
								_collector.emit("mp", input, new Values(orderDate, parentOrderId, 
										orderSource, provId, cityId, buyTimes, soItem));
							}
						}
					}
				}
			}
		}
		_collector.ack(input);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("grpon", new Fields("date", "orderid", "ordersource", "provid", "cityid", "buytimes","item"));
		declarer.declareStream("mp", new Fields("date", "orderid", "ordersource", "provid", "cityid", "buytimes", "item"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
