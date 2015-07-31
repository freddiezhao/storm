package com.yihaodian.bi.storm.business.so.orderinfo;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

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
 * by zhangsheng 2014-6-4 统计移动成交订单净额
 * 
 * 功能：获得JumpMQOrderVo成交订单信息,累加计算
 * update by zhangsheng 2014-6-18 去掉RocketMQ
 */
public class MobileOrderInfoBolt implements IRichBolt {

	private static final long serialVersionUID = 7525807650106935719L;

	private static final Logger logger = LoggerFactory
			.getLogger(MobileOrderInfoBolt.class);

	@Override
	public void cleanup() {

	}

	OutputCollector _collector;

	private GosOrderDao dao;
	JumpMQOrderVo jumpMQOrderVo = null;

	private static long insertDB_startTime = System.currentTimeMillis();
	private static long insertDB_endTime = 0;

	private static long insertDB_last_point_startTime = System
			.currentTimeMillis();
	private static long insertDB_last_point_endTime = 0;

	/** 线图-订单净额入库rowkey */
	private String rowkey_amount = null;
	/** 线图-订单净额最后累加值入库rowkey 重启bolt时需要读库获取最后累加值 */
	private String rowkey_amount_last = null;
	/** 当天日期 */
	private String curTime = null;
	/** 订单创建日期 */
	private String orderCreateDate = null;
	/** 订单净额累加值 */
	private static Double orderAmountTotal = 0.0;
	/** 坐标点信息 */
	private String[] strLastXValue = null;

	/** 柱状图-柱状图展示完整数据字符串rowkey */
	private String rowkey_amount_col_data = null;
	/** 当前小时数 */
	private int curHour;
	/** 订单创建-小时数 */
	private int orderCreateHour;
	/** 小时-订单净额累加值 */
	private static Double orderAmountHourTotal = 0.0;
	/** 柱状图展示 数据字符串 完整形式 */
	private String colData = null;
	/** 柱状图展示 用于拼接处理 */
	private static String sFinal = "[]";

	private int dealHourFlg = 0;

	/** 线图-订单数入库rowkey */
	private String rowkey_ordr_num = null;
	/** 线图-订单数最后累加值入库rowkey 重启bolt时需要读库获取最后累加值 */
	private String rowkey_ordr_num_last = null;
	/** 订单数累加值 */
	private static long orderNumTotal = 0;
	/** 小时-订单净额累加值 */
	private static long orderNumHourTotal = 0;

	/** 柱状图-柱状图展示完整数据字符串rowkey */
	private String rowkey_ordr_num_col_data = null;
	/** 柱状图展示 数据字符串 完整形式 */
	private String colDataOrdrNum = null;
	/** 柱状图展示 用于拼接处理 */
	private static String sFinalOrdrNum = "[]";

	private Date xTime = null;

	List<JumpMQOrderItemVo> soItemList = new ArrayList<JumpMQOrderItemVo>();

	@Override
	public void execute(Tuple input) {
		try {
			// 当前坐标点信息
			strLastXValue = CommonUtil.getCurFullXValueStr();
			jumpMQOrderVo = (JumpMQOrderVo) input.getValue(0);
			Double orderAmount = 0.0;
			// 获取订单创建时间
			if (jumpMQOrderVo == null) {// 移动数据
				throw new Exception();
			}
			/** 判断是否为取消订单 */
			if (jumpMQOrderVo.getOrderStatus() == 34) {
				logger.info("===取消订单信息===订单id:" + jumpMQOrderVo.getId()
						+ "--订单code:" + jumpMQOrderVo.getOrderCode()
						+ "--订单创建时间:" + jumpMQOrderVo.getOrderCreateTime()
						+ "--订单金额:" + jumpMQOrderVo.getOrderAmount());
				throw new Exception();
			}
			/** 非移动订单不处理 */
			if (jumpMQOrderVo.getOrderSource() != 3) {
				throw new Exception();
			}
			if (CommonUtil.isPayOnline(jumpMQOrderVo.getPayServiceType())) {// 在线支付
				if (jumpMQOrderVo.getOrderPaymentConfirmDate() != null
						&& !"".equals(jumpMQOrderVo
								.getOrderPaymentConfirmDate())) {
					orderCreateDate = DateUtil.transferDateToString(
							jumpMQOrderVo.getOrderPaymentConfirmDate(),
							"yyyyMMdd");
					orderCreateHour = jumpMQOrderVo
							.getOrderPaymentConfirmDate().getHours() + 1;
					/** 跨天时仍在处理前一天的数据,需特殊处理成当天的数据 */
					if ("00.00".equals(strLastXValue[0])) {
						orderCreateDate = DateUtil.transferDateToString(
								new Date(), "yyyyMMdd");
						orderCreateHour = new Date().getHours() + 1;
					}
				} else {
					// 如果支付时间为空，则算为当天的
					orderCreateDate = curTime;
					orderCreateHour = curHour;
				}
			} else {
				if (jumpMQOrderVo.getOrderCreateTime() != null
						&& !"".equals(jumpMQOrderVo.getOrderCreateTime())) {
					orderCreateDate = DateUtil.transferDateToString(
							jumpMQOrderVo.getOrderCreateTime(), "yyyyMMdd");
					orderCreateHour = jumpMQOrderVo.getOrderCreateTime()
							.getHours() + 1;
					/** 跨天时仍在处理前一天的数据,需特殊处理成当天的数据 */
					if ("00.00".equals(strLastXValue[0])) {
						orderCreateDate = DateUtil.transferDateToString(
								new Date(), "yyyyMMdd");
						orderCreateHour = new Date().getHours() + 1;
					}
				} else {
					// 如果创建时间为空，则算为当天的
					orderCreateDate = curTime;
					orderCreateHour = curHour;
				}
			}
			/** 最新统计口径 订单净额 - 满立减 - 抵用券 */
			if (jumpMQOrderVo.getChildOrderList()!=null && jumpMQOrderVo.getChildOrderList().size()>0) {
				for(JumpMQOrderVo childOrder:jumpMQOrderVo.getChildOrderList())
				{
					soItemList = childOrder.getSoItemList() ;
					for(JumpMQOrderItemVo soItemVo : soItemList)
					{
						if (soItemVo.getIsItemLeaf() != 1) {
							continue;
						}
						if(soItemVo.getPromotionAmount() == null)
						{
							soItemVo.setPromotionAmount(new BigDecimal(0.0));
						}
						if(soItemVo.getCouponAmount() == null)
						{
							soItemVo.setCouponAmount(new BigDecimal(0.0));
						}
						if(soItemVo.getOrderItemAmount() == null)
						{
							soItemVo.setOrderItemAmount(new BigDecimal(0.0));
						}
						orderAmount += (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
					}
				}
			}else {
				soItemList = jumpMQOrderVo.getSoItemList() ;
				for(JumpMQOrderItemVo soItemVo : soItemList)
				{
					if(soItemVo.getPromotionAmount() == null)
					{
						soItemVo.setPromotionAmount(new BigDecimal(0.0));
					}
					if(soItemVo.getCouponAmount() == null)
					{
						soItemVo.setCouponAmount(new BigDecimal(0.0));
					}
					if(soItemVo.getOrderItemAmount() == null)
					{
						soItemVo.setOrderItemAmount(new BigDecimal(0.0));
					}
					orderAmount += (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
				}
			}

			// 订单创建时间和当前系统时间是同一天则进行订单净额累加操作,否则进行跨天处理
			if (orderCreateDate.equals(curTime)) {
				orderAmountTotal += orderAmount;
				orderNumTotal++;
				dealHourFlg = 0;
			} else if (Integer.parseInt(curTime) < Integer
					.parseInt(orderCreateDate)) {
				logger.info("===跨天处理===" + orderCreateDate);
				/** 跨天处理 */
				curTime = orderCreateDate;
				curHour = orderCreateHour;

				/** 清空小时数据 */
				orderAmountHourTotal = 0.0;
				orderNumHourTotal = 0;

				orderAmountTotal = 0.0;
				rowkey_amount_last = "last_amount_" + curTime;
				rowkey_amount_col_data = "col_data_" + curTime;
				sFinal = "[]";

				orderNumTotal = 0;
				rowkey_ordr_num_last = "last_ordr_num_" + curTime;
				rowkey_ordr_num_col_data = "col_data_ordr_num_" + curTime;
				sFinalOrdrNum = "[]";

				dealHourFlg = 0;
			} else {
				logger.info("===异常：===订单id:" + jumpMQOrderVo.getId()
						+ "===订单code:" + jumpMQOrderVo.getOrderCode()
						+ "===时间:" + orderCreateDate);
				dealHourFlg = 1;
			}

			// 订单创建小时数和当前系统小时数相同则进行小时订单净额累加操作,否则进行跨小时处理
			if (dealHourFlg == 0) {// 消息源会有时候当天传过来昨天或更早的数据,这个时候我们只处理当天的数据
				if (curHour == orderCreateHour) {
					// 如果sFinal为空,则初始化
					if ("[]".equals(sFinal)) {
						sFinal = CommonUtil.appendNextHourStr(curHour, sFinal,
								"0");
					}

					orderAmountHourTotal += orderAmount;
					orderNumHourTotal++;

					// 柱状图当前小时数据更新,之后需要重新设值
					sFinal = CommonUtil.replaceNowHourStr(sFinal,
							orderAmountHourTotal.toString());
					colData = sFinal;

					if ("[]".equals(sFinalOrdrNum)) {
						sFinalOrdrNum = CommonUtil.appendNextHourStr(curHour,
								sFinalOrdrNum, "0");
					}
					sFinalOrdrNum = CommonUtil.replaceNowHourStr(sFinalOrdrNum,
							String.valueOf(orderNumHourTotal));
					colDataOrdrNum = sFinalOrdrNum;
				} else if (curHour < orderCreateHour) {// 需要强制入库,否则会丢失至多30秒的数据
					logger.info("===跨小时处理===" + orderCreateHour);
					/** 跨小时处理 */
					curHour = orderCreateHour;

					orderAmountHourTotal = 0.0;
					// 柱状图拼接新的小时数据
					sFinal = CommonUtil.appendNextHourStr(curHour, sFinal,
							orderAmountHourTotal.toString());
					colData = sFinal;

					orderNumHourTotal = 0;
					sFinalOrdrNum = CommonUtil.appendNextHourStr(curHour,
							sFinalOrdrNum, String.valueOf(orderNumHourTotal));
					colDataOrdrNum = sFinalOrdrNum;
				} else {// 当前时间大于订单创建时间,表示mq里面有之前阻塞的订单,需要特殊处理或忽略
					logger
							.info("===处理阻塞订单===订单id:" + jumpMQOrderVo.getId()
									+ "---订单code:"
									+ jumpMQOrderVo.getOrderCode()
									+ "---订单创建时间:"
									+ jumpMQOrderVo.getOrderCreateTime());
					// 柱状图拼接新的小时数据
					sFinal = CommonUtil.replaceBeforeHourStr(sFinal,
							orderAmount, orderCreateHour);
					colData = sFinal;
					logger.info("colData before add===" + colData);

					sFinalOrdrNum = CommonUtil.replaceOrdrNumBeforeHourStr(
							sFinalOrdrNum, orderCreateHour);
					colDataOrdrNum = sFinalOrdrNum;
				}
			}

			/** 写habse */
			synchronized (this) {
				insertDB_last_point_endTime = System.currentTimeMillis();
				// 5秒写入一次最后的点,前端5秒读取一次刷新最新的值insertDB_last_point_startTime
				if (insertDB_last_point_endTime - insertDB_last_point_startTime >= Constant.SECOND_5) {
					/** 移动订单净额 */
					// 插入最后累加值记录,重启bolt的时候需要获取最后一次插入的金额,在此基础上再进行累加
					dao
							.insertGosOrder(
									Constant.TABLE_MOBILE_ORDER_INFO_RESULT,
									rowkey_amount_last,
									Constant.COMMON_FAMILY,
									new String[] { "order_amount", "xValue",
											"xTitle" },
									new String[] { orderAmountTotal + "",
											strLastXValue[1], strLastXValue[0] });

					// 插入拼接的柱状图数据,单条记录,后端重启和前端展示获取历史都从这里获取
					dao.insertGosOrder(
							Constant.TABLE_MOBILE_ORDER_INFO_RESULT,
							rowkey_amount_col_data,
							Constant.COMMON_FAMILY, "col_data", colData);

					/** 移动订单数 */
					dao.insertGosOrder(
							Constant.TABLE_MOBILE_ORDER_INFO_RESULT,
							rowkey_ordr_num_last, Constant.COMMON_FAMILY,
							new String[] { "order_num", "xValue", "xTitle" },
							new String[] { orderNumTotal + "",
									strLastXValue[1], strLastXValue[0] });

					dao.insertGosOrder(
							Constant.TABLE_MOBILE_ORDER_INFO_RESULT,
							rowkey_ordr_num_col_data,
							Constant.COMMON_FAMILY, "col_data_ordr_num",
							colDataOrdrNum);

					insertDB_last_point_startTime = System.currentTimeMillis();
				}
			}

			synchronized (this) {
				insertDB_endTime = System.currentTimeMillis();
				// 30秒写入一次
				if (insertDB_endTime - insertDB_startTime >= Constant.SECOND_50) {
					try {
						rowkey_amount = curTime + "_amount_"
								+ System.currentTimeMillis();
						// 插入点的历史数据,线图中用来拼接历史数据线条
						dao.insertGosOrder(
								Constant.TABLE_MOBILE_ORDER_INFO_RESULT,
								rowkey_amount, Constant.COMMON_FAMILY,
								new String[] { "order_amount", "xValue",
										"xTitle" }, new String[] {
										orderAmountTotal + "",
										strLastXValue[1], strLastXValue[0] });

						rowkey_ordr_num = curTime + "_ordr_num_"
								+ System.currentTimeMillis();
						dao
								.insertGosOrder(
										Constant.TABLE_MOBILE_ORDER_INFO_RESULT,
										rowkey_ordr_num,
										Constant.COMMON_FAMILY,
										new String[] { "order_num", "xValue",
												"xTitle" }, new String[] {
												orderNumTotal + "",
												strLastXValue[1],
												strLastXValue[0] });

						logger.info("===30秒入库结束===");
						insertDB_startTime = System.currentTimeMillis();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		} catch (Exception e) {
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
		try {
			/** 初始化以下值 */
			dao = new GosOrderDaoImpl();

			curTime = DateUtil.transferDateToString(new Date(), "yyyyMMdd");
			curHour = DateUtil.getCurHour() + 1;
			xTime = new Date();

			rowkey_amount_last = "last_amount_" + curTime;
			rowkey_amount_col_data = "col_data_" + curTime;
			/** 读取Hbase获取 */
			String last_amount_value = dao.getValueByCol(
					Constant.TABLE_MOBILE_ORDER_INFO_RESULT,
					rowkey_amount_last, "order_amount");
			String last_amount_col_data_value = dao.getValue(
					Constant.TABLE_MOBILE_ORDER_INFO_RESULT,
					rowkey_amount_col_data);
			if (!"".equals(last_amount_value)) {
				orderAmountTotal = Double.parseDouble(last_amount_value);
			}
			if (!"".equals(last_amount_col_data_value)) {
				sFinal = last_amount_col_data_value;
			}

			/**
			 * 订单数
			 */
			rowkey_ordr_num_last = "last_ordr_num_" + curTime;
			rowkey_ordr_num_col_data = "col_data_ordr_num_" + curTime;
			/** 读取Hbase获取 */
			String last_ordr_num_value = dao.getValueByCol(
					Constant.TABLE_MOBILE_ORDER_INFO_RESULT,
					rowkey_ordr_num_last, "order_num");
			String last_ordr_num_col_data_value = dao.getValue(
					Constant.TABLE_MOBILE_ORDER_INFO_RESULT,
					rowkey_ordr_num_col_data);
			if (!"".equals(last_ordr_num_value)) {
				orderNumTotal = Long.parseLong(last_ordr_num_value);
			}
			if (!"".equals(last_ordr_num_col_data_value)) {
				sFinalOrdrNum = last_ordr_num_col_data_value;
			}

			/**
			 * 这里补充:如果在sFinal中找不到当前小时的字符串,则进行appendNextHourStr的操作。由于重启过程中,跨小时了,这个时候curHour ==
			 * orderCreateHour,进行replaceNowHourStr操作,不会在当前小时的柱子上进行添加,所以需要补充一个小时的柱子
			 */
			logger.info("===index hour==="
					+ sFinal.indexOf("[" + curHour + ","));
			if (sFinal.indexOf("[" + curHour + ",") == -1) {
				logger.info("===init sFinal===");
				sFinal = CommonUtil.appendNextHourStr(curHour, sFinal, "0.0");
				sFinalOrdrNum = CommonUtil.appendNextHourStr(curHour,
						sFinalOrdrNum, "0");
			}
			
			/** 获取柱状图最后一个小时的数值用来作为初始值 */
			orderAmountHourTotal = Double.parseDouble(sFinal.substring(sFinal.lastIndexOf(",")+1,sFinal.length()-2));
			orderNumHourTotal = Long.parseLong(sFinalOrdrNum.substring(sFinalOrdrNum.lastIndexOf(",")+1,sFinalOrdrNum.length()-2));

			logger.info("===curHour===" + curHour);
			logger.info("amount init value is:   " + orderAmountTotal);
			logger.info("col data init value is:   " + sFinal);
			logger.info("ordr num init value is:   " + orderNumTotal);
			logger.info("col data ordr num init value is:   " + sFinalOrdrNum);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
