package com.yihaodian.bi.storm.business.so.orderinfo;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.CommonUtil;
import com.yihaodian.bi.storm.common.util.Constant;
import com.yihaodian.bi.storm.common.util.Constant;

/**
 * @author zhangsheng2 TODO 城市订单净额 北上广深
 */
public class CoreCityAmtBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory
			.getLogger(CoreCityAmtBolt.class);

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	private GosOrderDao dao;
	JumpMQOrderVo jumpMQOrderVo = null;
	JumpMQOrderItemVo itemVo = null;
	List<JumpMQOrderItemVo> soItemList = new ArrayList<JumpMQOrderItemVo>();

	private static long beginTime = System.currentTimeMillis();
	private static long endTime = 0;
	private static String orderDate = null;

	OutputCollector collector = null;
	String messageString = null;

	/** 北京 */
	private static Double orderAmountBJ = 0.0;
	/** 上海 */
	private static Double orderAmountSH = 0.0;
	/** 广东 */
	private static Double orderAmountGD = 0.0;
	/** 深圳 */
	private static Double orderAmountSZ = 0.0;
	String curDate = null; // 用于跨天判断
	Map<String, Double> countMap = null;

	private String regn = "1";
	
	private static Double orderAmountTotal = 0.0;

	@Override
	public void execute(Tuple input) {
		jumpMQOrderVo = (JumpMQOrderVo) input.getValue(0);// 多线程下，input.getValue(0)的某个值不会被多个线程重复获取，即线程安全
		Double orderAmount = 0.0;
		try {
			if (jumpMQOrderVo == null)
				throw new Exception("jumpMQOrderVo is null");

			/** 判断是否为取消订单 */
			if (jumpMQOrderVo.getOrderStatus() == 34) {
				logger.info("===取消订单信息===订单id:" + jumpMQOrderVo.getId()
						+ "--订单code:" + jumpMQOrderVo.getOrderCode()
						+ "--订单创建时间:" + jumpMQOrderVo.getOrderCreateTime()
						+ "--订单金额:" + jumpMQOrderVo.getOrderAmount());
				throw new Exception();
			}
			
			if (CommonUtil.isPayOnline(jumpMQOrderVo.getPayServiceType())) {// 在线支付的取支付时间
				if (jumpMQOrderVo.getOrderPaymentConfirmDate() != null
						&& !"".equals(jumpMQOrderVo
								.getOrderPaymentConfirmDate())) {
					orderDate = DateUtil.getFmtDate(jumpMQOrderVo
							.getOrderPaymentConfirmDate(), DateUtil.YYYYMMDD_STR);
				} else {
					orderDate = curDate;
				}
			} else {
				if (jumpMQOrderVo.getOrderCreateTime() != null
						&& !"".equals(jumpMQOrderVo.getOrderCreateTime())) {
					orderDate = DateUtil.getFmtDate(jumpMQOrderVo
							.getOrderCreateTime(), DateUtil.YYYYMMDD_STR);
				} else {
					orderDate = curDate;
				}
			}
			if (!curDate.equals(orderDate)
					&& DateUtil.getDate(orderDate, DateUtil.YYYYMMDD_STR).after(
							DateUtil.getDate(curDate, DateUtil.YYYYMMDD_STR))) {
				// 跨天
				curDate = orderDate; // 更新当前日期
				orderAmountTotal = 0.0;
				logger
						.info("kua tian qing kong qian============================"
								+ countMap.get(curDate + "_" + 1));
				countMap.clear();
			} else {
				// 取城市匹配  good_receive_province_id 2 1 
				// good_receive_city_id 1000 1 237 238 北京 上海 广州 深圳
				long cityId = jumpMQOrderVo.getGoodReceiverCityId();
				if (cityId == 1) {
					regn = "1";// 上海
				} else if (cityId == 1000) {
					regn = "2";// 北京
				} else if (cityId == 237) {
					regn = "3";// 广州
				} else if (cityId == 238) {
					regn = "4";// 深圳
				} else {
					regn = "5";// 其他
				}

				// 判断是否有子单,如果有子单,soItem在子单中，记录产品和金额
				if (jumpMQOrderVo.getChildOrderList() != null
						&& jumpMQOrderVo.getChildOrderList().size() > 0) {
					for (JumpMQOrderVo childOrder : jumpMQOrderVo
							.getChildOrderList()) {
						soItemList = childOrder.getSoItemList();
						// 累加金额
						// 写map
						for (JumpMQOrderItemVo soItemVo : soItemList) {
							if (soItemVo.getIsItemLeaf() != 1) {
								continue;
							}
							Double eachAmt = countMap.get(orderDate + "_" + regn);
							if (eachAmt == null) {
								eachAmt = 0.0;
							}
							if (soItemVo.getPromotionAmount() == null) {
								soItemVo
										.setPromotionAmount(new BigDecimal(0.0));
							}
							if (soItemVo.getCouponAmount() == null) {
								soItemVo.setCouponAmount(new BigDecimal(0.0));
							}
							if (soItemVo.getOrderItemAmount() == null) {
								soItemVo
										.setOrderItemAmount(new BigDecimal(0.0));
							}
							orderAmount += (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
							eachAmt += (soItemVo.getOrderItemAmount()
									.doubleValue()
									- soItemVo.getPromotionAmount()
											.doubleValue() - soItemVo
									.getCouponAmount().doubleValue());
							countMap.put(orderDate + "_" + regn, eachAmt);
						}
					}
				} else {
					soItemList = jumpMQOrderVo.getSoItemList();
					for (JumpMQOrderItemVo soItemVo : soItemList) {
						// 累加金额
						// 写map
						Double eachAmt = countMap.get(orderDate + "_" + regn);
						if (eachAmt == null) {
							eachAmt = 0.0;
						}
						if (soItemVo.getPromotionAmount() == null) {
							soItemVo.setPromotionAmount(new BigDecimal(0.0));
						}
						if (soItemVo.getCouponAmount() == null) {
							soItemVo.setCouponAmount(new BigDecimal(0.0));
						}
						if (soItemVo.getOrderItemAmount() == null) {
							soItemVo.setOrderItemAmount(new BigDecimal(0.0));
						}
						orderAmount += (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
						eachAmt += (soItemVo.getOrderItemAmount().doubleValue()
								- soItemVo.getPromotionAmount().doubleValue() - soItemVo
								.getCouponAmount().doubleValue());
						countMap.put(orderDate + "_" + regn, eachAmt);
					}
				}
				orderAmountTotal += orderAmount;

				synchronized (this) {
					endTime = System.currentTimeMillis();
					if (endTime - beginTime >= Constant.SECOND_30 / 6) {
						try {
							insertData(countMap, dao);
							logger
									.info("write HBase, shanghai amt is ============================"
											+ countMap.get(curDate + "_" + 1));
							
							dao.insertGosOrder(
									Constant.TABLE_CORE_CITY_ORDERINFO_RSLT,
									"last_amount_" + curDate,
									Constant.COMMON_FAMILY, new String[] {
											"order_amount"},
									new String[] {orderAmountTotal+""});
						} catch (Exception e) {
							e.printStackTrace();
						}
						// 更新开始时间
						beginTime = System.currentTimeMillis();
					}
				}
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		try {
			dao = new GosOrderDaoImpl();

			this.collector = collector;

			curDate = DateUtil.getFmtDate(null, DateUtil.YYYYMMDD_STR);

			countMap = initMap(curDate, dao);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	private void pl(Object o) {
		System.out.println(o);
		logger.info(o.toString());
	}

	public void insertData(Map<String, Double> regnAmountMap, GosOrderDao dao) {
		Iterator<String> iterator = regnAmountMap.keySet().iterator();
		while (iterator.hasNext()) {
			String key = iterator.next();
			try {
				dao.insertRecord(Constant.TABLE_CORE_CITY_ORDERINFO_RSLT,
						key, "cf", "pro_amount", regnAmountMap.get(key) + "");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public Map<String, Double> initMap(String rowKey, GosOrderDao dao)
			throws Exception {
		Map<String, Double> map = new HashMap<String, Double>();
		List<Result> rsList = dao.getRecordByRowKeyRegex(
				Constant.TABLE_CORE_CITY_ORDERINFO_RSLT, rowKey);
		for (Result result : rsList) {
			String rowKeyString = new String(result.getRow());
			for (KeyValue keyValue : result.raw()) {
				if ("pro_amount".equals(new String(keyValue.getQualifier()))) {
					map.put(rowKeyString, Double.parseDouble(new String(
							keyValue.getValue())));
					break;
				}
			}
		}
		return map;
	}

}
