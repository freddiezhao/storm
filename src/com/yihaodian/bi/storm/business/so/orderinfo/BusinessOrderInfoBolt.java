package com.yihaodian.bi.storm.business.so.orderinfo;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
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
import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.CommonUtil;
import com.yihaodian.bi.storm.common.util.Constant;
import com.yihaodian.bi.storm.common.util.Constant;

/**
 * by zhangsheng 2014-5-20 统计成交订单净额
 * 
 * 功能：获得JumpMQOrderVo成交订单信息,累加计算
 * update by zhangsheng 2014-6-18 去掉RocketMQ
 */
public class BusinessOrderInfoBolt implements IRichBolt {

	private static final long serialVersionUID = 7525807650106935719L;

	private static final Logger logger = LoggerFactory
			.getLogger(BusinessOrderInfoBolt.class);

	@Override
	public void cleanup() {
		try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	DBConnection dBConnection = null;
	Connection con = null;
	OutputCollector _collector;

	private GosOrderDao dao;
	JumpMQOrderVo jumpMQOrderVo = null;

	private static long insertDB_startTime = System.currentTimeMillis();
	private static long insertDB_endTime = 0;
	
	private static long insertDB_last_point_startTime = System.currentTimeMillis();
	private static long insertDB_last_point_endTime = 0;

	/** 线图-订单净额入库rowkey */
//	private String rowkey_amount = null;
	private String rowkey_amount_sc = null;
	private String rowkey_amount_zy = null;
	/** 线图-订单净额最后累加值入库rowkey 重启bolt时需要读库获取最后累加值 */
//	private String rowkey_amount_last = null;
	private String rowkey_amount_last_sc = null;
	private String rowkey_amount_last_zy = null;
	/** 当天日期 */
	private String curTime = null;
	/** 订单创建日期 */
	private String orderCreateDate = null;
	/** 订单净额累加值 */
//	private static Double orderAmountTotal = 0.0;
	private static Double orderAmountScTotal = 0.0;
	private static Double orderAmountZyTotal = 0.0;
	/** 坐标点信息 */
	private String[] strLastXValue = null;

	// /** 线柱状图-订单净额最后累加值入库rowkey 重启bolt时需要读库获取最后累加值 */
	// private String rowkey_amount_col_last = null;
	/** 柱状图-柱状图展示完整数据字符串rowkey */
//	private String rowkey_amount_col_data = null;
	private String rowkey_amount_col_data_sc = null;
	private String rowkey_amount_col_data_zy = null;
	// /** 柱状图-最后的累加值 */
	// private String rowkey_amount_col_last = null;
	/** 柱状图-每小时最后的累加值 */
	private String rowkey_amount_hour_last = null;
	/** 当前小时数 */
	private int curHour;
	/** 订单创建-小时数 */
	private int orderCreateHour;
	/** 小时-订单净额累加值 */
//	private static Double orderAmountHourTotal = 0.0;
	private static Double orderAmountHourScTotal = 0.0;
	private static Double orderAmountHourZyTotal = 0.0;
	/** 柱状图展示 数据字符串 完整形式 */
//	private String colData = null;
	private String colDataSc = null;
	private String colDataZy = null;
	/** 柱状图展示 用于拼接处理 */
//	private static String sFinal = "[]";
	
	/** 柱状图展示 用于拼接处理 */
	private static String sFinalSc = "[]";
	
	/** 柱状图展示 用于拼接处理 */
	private static String sFinalZy = "[]";

	private int dealHourFlg = 0;

	/** 线图-订单数入库rowkey */
//	private String rowkey_ordr_num = null;
	private String rowkey_ordr_num_sc = null;
	private String rowkey_ordr_num_zy = null;
	/** 线图-订单数最后累加值入库rowkey 重启bolt时需要读库获取最后累加值 */
//	private String rowkey_ordr_num_last = null;
	private String rowkey_ordr_num_last_sc = null;
	private String rowkey_ordr_num_last_zy = null;
	/** 订单数累加值 */
//	private static long orderNumTotal = 0;
	private static long orderNumTotalSc = 0;
	private static long orderNumTotalZy = 0;
	
	/** 小时-订单净额累加值 */
	private static long orderNumHourTotal = 0;

	/** 柱状图-柱状图展示完整数据字符串rowkey */
	private String rowkey_ordr_num_col_data = null;
	/** 柱状图-每小时最后的累加值 */
	private String rowkey_ordr_num_hour_last = null;
	/** 柱状图展示 数据字符串 完整形式 */
	private String colDataOrdrNum = null;
	/** 柱状图展示 用于拼接处理 */
	private static String sFinalOrdrNum = "[]";

	private Date xTime = null;

	List<JumpMQOrderItemVo> soItemList = new ArrayList<JumpMQOrderItemVo>();
	
	private static Double testItemAmountTotal = 0.0;
	
//	private static Double testZkAmountTotal = 0.0;
	
	@Override
	public void execute(Tuple input) {
		try {
			// 当前坐标点信息
			strLastXValue = CommonUtil.getCurFullXValueStr();
			jumpMQOrderVo = (JumpMQOrderVo) input.getValue(0);// 多线程下，input.getValue(0)的某个值不会被多个线程重复获取，即线程安全
//			Double orderAmount = 0.0;
			Double orderAmountSc = 0.0;
			Double orderAmountZy = 0.0;
			Long orderNumSc = 0l;
			Long orderNumZy = 0l;
			// 获取订单创建时间
			if (jumpMQOrderVo == null) {
				logger.info("====空消息体===");
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
			if (CommonUtil.isPayOnline(jumpMQOrderVo.getPayServiceType())) {// 在线支付
				if (jumpMQOrderVo.getOrderPaymentConfirmDate() != null
						&& !"".equals(jumpMQOrderVo
								.getOrderPaymentConfirmDate())) {
					orderCreateDate = DateUtil.transferDateToString(
							jumpMQOrderVo.getOrderPaymentConfirmDate(),
							"yyyyMMdd");
					orderCreateHour = jumpMQOrderVo
							.getOrderPaymentConfirmDate().getHours() + 1;
					/** 跨天时仍在处理前一天的数据,需特殊处理 */
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
					/** 跨天时仍在处理前一天的数据,需特殊处理 */
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
			/** 老的统计口径*/
//			if(null!=jumpMQOrderVo.getOrderAmount()){
//				orderAmount = jumpMQOrderVo.getOrderAmount().doubleValue();
//			}
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
						String bizUnit = this.getBusIDFromMer(dao, soItemVo.getMerchantId()+"");
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
//						orderAmount += (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
						if("1".equals(bizUnit)){//自营
							orderAmountZy += (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
							orderNumZy = 1l;
						}
						if("3".equals(bizUnit)){//商城
							orderAmountSc += (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
							orderNumSc = 1l;
						}
//						testZkAmountTotal += (soItemVo.getPromotionAmount().doubleValue() + soItemVo.getCouponAmount().doubleValue()) ;
					}
				}
			}else {
				soItemList = jumpMQOrderVo.getSoItemList() ;
				for(JumpMQOrderItemVo soItemVo : soItemList)
				{
					String bizUnit = this.getBusIDFromMer(dao, soItemVo.getMerchantId()+"");
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
//					orderAmount += (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
					if("1".equals(bizUnit)){//自营
						orderAmountZy += (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
						orderNumZy = 1l;
					}
					if("3".equals(bizUnit)){//商城
						orderAmountSc += (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
						orderNumSc = 1l;
					}
//					testZkAmountTotal += (soItemVo.getPromotionAmount().doubleValue() + soItemVo.getCouponAmount().doubleValue());
				}
			}
			
			// 订单创建时间和当前系统时间是同一天则进行订单净额累加操作,否则进行跨天处理
			if (orderCreateDate.equals(curTime)) {
//				orderAmountTotal += orderAmount;
				orderAmountScTotal += orderAmountSc;
				orderAmountZyTotal += orderAmountZy;
				orderNumTotalSc += orderNumSc;
				orderNumTotalZy += orderNumZy;
//				orderNumTotal++;
				dealHourFlg = 0;
			} else if (Integer.parseInt(curTime) < Integer
					.parseInt(orderCreateDate)) {
				logger.info("===跨天处理===" + orderCreateDate);
				/** 跨天处理 */
				curTime = orderCreateDate;
				curHour = orderCreateHour;

				/** 清空小时数据 */
//				orderAmountHourTotal = 0.0;
				orderAmountHourScTotal = 0.0;
				orderAmountHourZyTotal = 0.0;
				orderNumHourTotal = 0;

//				orderAmountTotal = 0.0;
				orderAmountScTotal = 0.0;
				orderAmountZyTotal = 0.0;
//				rowkey_amount_last = "last_amount_" + curTime;
				rowkey_amount_last_sc = "last_amount_sc_" + curTime;
				rowkey_amount_last_zy = "last_amount_zy_" + curTime;
//				rowkey_amount_col_data = "col_data_" + curTime;
				rowkey_amount_col_data_sc = "col_data_sc_" + curTime;
				rowkey_amount_col_data_zy = "col_data_zy_" + curTime;
//				sFinal = "[]";
				sFinalSc = "[]";
				sFinalZy = "[]";

//				orderNumTotal = 0;
				orderNumTotalSc = 0l;
				orderNumTotalZy = 0l;
//				rowkey_ordr_num_last = "last_ordr_num_" + curTime;
				rowkey_ordr_num_last_sc = "last_ordr_num_sc_" + curTime;
				rowkey_ordr_num_last_zy = "last_ordr_num_zy_" + curTime;
				rowkey_ordr_num_col_data = "col_data_ordr_num_" + curTime;
				sFinalOrdrNum = "[]";
				
				//test
				testItemAmountTotal = 0.0;
//				testZkAmountTotal = 0.0;

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
//					if ("[]".equals(sFinal)) {
//						sFinal = CommonUtil.appendNextHourStr(curHour, sFinal,
//								"0");
//					}
					if ("[]".equals(sFinalSc)) {
						sFinalSc = CommonUtil.appendNextHourStr(curHour, sFinalSc,
								"0");
					}
					if ("[]".equals(sFinalZy)) {
						sFinalZy = CommonUtil.appendNextHourStr(curHour, sFinalZy,
								"0");
					}
//					orderAmountHourTotal += orderAmount;
					orderAmountHourScTotal += orderAmountSc;
					orderAmountHourZyTotal += orderAmountZy;
//					orderNumHourTotal++;

					// 柱状图当前小时数据更新,之后需要重新设值
//					sFinal = CommonUtil.replaceNowHourStr(sFinal,
//							orderAmountHourTotal.toString());
					sFinalSc = CommonUtil.replaceNowHourStr(sFinalSc,
							orderAmountHourScTotal.toString());
					sFinalZy = CommonUtil.replaceNowHourStr(sFinalZy,
							orderAmountHourZyTotal.toString());
//					colData = sFinal;
					colDataSc = sFinalSc;
					colDataZy = sFinalZy;

//					if ("[]".equals(sFinalOrdrNum)) {
//						sFinalOrdrNum = CommonUtil.appendNextHourStr(curHour,
//								sFinalOrdrNum, "0");
//					}
//					sFinalOrdrNum = CommonUtil.replaceNowHourStr(sFinalOrdrNum,
//							String.valueOf(orderNumHourTotal));
//					colDataOrdrNum = sFinalOrdrNum;
				} else if (curHour < orderCreateHour) {// 需要强制入库,否则会丢失至多30秒的数据
					logger.info("===跨小时处理===" + orderCreateHour);
					/** 跨小时处理 */
					curHour = orderCreateHour;

//					orderAmountHourTotal = 0.0;
					orderAmountHourScTotal = 0.0;
					orderAmountHourZyTotal = 0.0;
					// 柱状图拼接新的小时数据
//					sFinal = CommonUtil.appendNextHourStr(curHour, sFinal,
//							orderAmountHourTotal.toString());
					sFinalSc = CommonUtil.appendNextHourStr(curHour, sFinalSc,
							orderAmountHourScTotal.toString());
					sFinalZy = CommonUtil.appendNextHourStr(curHour, sFinalZy,
							orderAmountHourZyTotal.toString());
//					colData = sFinal;
					colDataSc = sFinalSc;
					colDataZy = sFinalZy;

//					orderNumHourTotal = 0;
//					sFinalOrdrNum = CommonUtil.appendNextHourStr(curHour,
//							sFinalOrdrNum, String.valueOf(orderNumHourTotal));
//					colDataOrdrNum = sFinalOrdrNum;
				} else {// 当前时间大于订单创建时间,表示mq里面有之前阻塞的订单,需要特殊处理或忽略
					logger
							.info("===处理阻塞订单===订单id:" + jumpMQOrderVo.getId()
									+ "---订单code:"
									+ jumpMQOrderVo.getOrderCode()
									+ "---订单创建时间:"
									+ jumpMQOrderVo.getOrderCreateTime());
				}
			}

			/** 写habse */
			// 插入小时级最后累加值记录,30秒一次入库，如果期间有很多订单过来，30秒期间刚好碰到跨小时的情况的话，那部分数据就会丢失
			synchronized (this) {
				insertDB_last_point_endTime = System.currentTimeMillis();
				//5秒写入一次最后的点,前端5秒读取一次刷新最新的值insertDB_last_point_startTime
				if (insertDB_last_point_endTime - insertDB_last_point_startTime >= Constant.SECOND_5) {
					/** 订单净额 */
					// 插入最后累加值记录,重启bolt的时候需要获取最后一次插入的金额,在此基础上再进行累加
					dao.insertGosOrder(
							Constant.TABLE_BUS_ORDER_INFO_RESULT,
							rowkey_amount_last_sc,
							Constant.COMMON_FAMILY, new String[] {
									"order_amount", "xValue", "xTitle" },
							new String[] { orderAmountScTotal + "",
									strLastXValue[1], strLastXValue[0] });
					
					dao.insertGosOrder(
							Constant.TABLE_BUS_ORDER_INFO_RESULT,
							rowkey_amount_last_zy,
							Constant.COMMON_FAMILY, new String[] {
									"order_amount", "xValue", "xTitle" },
							new String[] { orderAmountZyTotal + "",
									strLastXValue[1], strLastXValue[0] });
					
					// 插入拼接的柱状图数据,单条记录,后端重启和前端展示获取历史都从这里获取
					dao.insertGosOrder(
							Constant.TABLE_BUS_ORDER_INFO_RESULT,
							rowkey_amount_col_data_sc,
							Constant.COMMON_FAMILY, "col_data",
							colDataSc);
					
					dao.insertGosOrder(
							Constant.TABLE_BUS_ORDER_INFO_RESULT,
							rowkey_amount_col_data_zy,
							Constant.COMMON_FAMILY, "col_data",
							colDataZy);
					
//					/** 订单数 */
					dao.insertGosOrder(
							Constant.TABLE_BUS_ORDER_INFO_RESULT,
							rowkey_ordr_num_last_sc,
							Constant.COMMON_FAMILY, new String[] {
									"order_num", "xValue", "xTitle" },
							new String[] { orderNumTotalSc + "",
									strLastXValue[1], strLastXValue[0] });
					
					dao.insertGosOrder(
							Constant.TABLE_BUS_ORDER_INFO_RESULT,
							rowkey_ordr_num_last_zy,
							Constant.COMMON_FAMILY, new String[] {
									"order_num", "xValue", "xTitle" },
							new String[] { orderNumTotalZy + "",
									strLastXValue[1], strLastXValue[0] });

//					dao.insertGosOrder(
//							TableContants.TABLE_BUS_ORDER_INFO_RESULT,
//							rowkey_ordr_num_col_data,
//							TableContants.COMMON_FAMILY,
//							"col_data_ordr_num", colDataOrdrNum);
					
					//满立减折扣数据验证
					
//					dao.insertGosOrder(
//							TableContants.TABLE_BUS_ORDER_INFO_RESULT,
//							"last_zk_amount_" + curTime,
//							TableContants.COMMON_FAMILY, new String[] {
//									"zk_order_amount", "xValue", "xTitle" },
//							new String[] { testZkAmountTotal + "",
//									strLastXValue[1], strLastXValue[0] });
					
//					dao.insertGosOrder(
//							TableContants.TABLE_BUS_ORDER_INFO_RESULT,
//							"last_item_amount_" + curTime,
//							TableContants.COMMON_FAMILY, new String[] {
//									"zk_order_amount", "xValue", "xTitle" },
//							new String[] { testItemAmountTotal + "",
//									strLastXValue[1], strLastXValue[0] });
					
					insertDB_last_point_startTime = System.currentTimeMillis();
				}
				
			}
			
			synchronized (this) {
				insertDB_endTime = System.currentTimeMillis();
				if (insertDB_endTime - insertDB_startTime >= Constant.SECOND_50) {
					/**
					 * 线图数据处理
					 */
//					rowkey_amount = curTime + "_amount_"
//							+ System.currentTimeMillis();
					rowkey_amount_sc = curTime + "_amount_sc_"
					+ System.currentTimeMillis();
					rowkey_amount_zy = curTime + "_amount_zy_"
					+ System.currentTimeMillis();
					// 插入点的历史数据,线图中用来拼接历史数据线条
					dao
							.insertGosOrder(
									Constant.TABLE_BUS_ORDER_INFO_RESULT,
									rowkey_amount_sc,
									Constant.COMMON_FAMILY,
									new String[] { "order_amount", "xValue",
											"xTitle" },
									new String[] { orderAmountScTotal + "",
											strLastXValue[1], strLastXValue[0] });
					
					dao
					.insertGosOrder(
							Constant.TABLE_BUS_ORDER_INFO_RESULT,
							rowkey_amount_zy,
							Constant.COMMON_FAMILY,
							new String[] { "order_amount", "xValue",
									"xTitle" },
							new String[] { orderAmountZyTotal + "",
									strLastXValue[1], strLastXValue[0] });

//					rowkey_ordr_num = curTime + "_ordr_num_"
//							+ System.currentTimeMillis();
					rowkey_ordr_num_sc = curTime + "_ordr_num_sc_"
					+ System.currentTimeMillis();
					rowkey_ordr_num_zy = curTime + "_ordr_num_zy_"
					+ System.currentTimeMillis();
					dao.insertGosOrder(Constant.TABLE_BUS_ORDER_INFO_RESULT,
							rowkey_ordr_num_sc, Constant.COMMON_FAMILY,
							new String[] { "order_num", "xValue", "xTitle" },
							new String[] { orderNumTotalSc + "",
									strLastXValue[1], strLastXValue[0] });
					
					dao.insertGosOrder(Constant.TABLE_BUS_ORDER_INFO_RESULT,
							rowkey_ordr_num_zy, Constant.COMMON_FAMILY,
							new String[] { "order_num", "xValue", "xTitle" },
							new String[] { orderNumTotalZy + "",
									strLastXValue[1], strLastXValue[0] });

					logger.info("===50秒入库成功===");
					insertDB_startTime = System.currentTimeMillis();
					
					
					//写Oracle ，推送数据到手机端报表
//					insertOracle(con,orderAmountTotal,orderNumTotal);
				}
			}
		} catch (Exception e) {
		}
	}
	
	public String getBusIDFromMer(GosOrderDao dao,String mrchnt_id)
	{
		String bizUnit = null;
		try {
			Result result = dao.getOneRecord("dim_mrchnt", mrchnt_id);
			for (KeyValue keyValue : result.raw()) {
				if ("biz_unit".equals(new String(keyValue.getQualifier()))) {
					bizUnit = new String(keyValue.getValue()) ;
					break;
				}
			}
		} catch (IOException e) {
			logger.error("dim_mrchnt", e);
		}
		return bizUnit;
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

//			rowkey_amount_last = "last_amount_" + curTime;
			rowkey_amount_last_sc = "last_amount_sc_" + curTime;
			rowkey_amount_last_zy = "last_amount_zy_" + curTime;
//			rowkey_amount_col_data = "col_data_" + curTime;
			rowkey_amount_col_data_sc = "col_data_sc_" + curTime;
			rowkey_amount_col_data_zy = "col_data_zy_" + curTime;
			/** 读取Hbase获取 */
//			String last_amount_value = dao.getValueByCol(
//					TableContants.TABLE_BUS_ORDER_INFO_RESULT, rowkey_amount_last,
//					"order_amount");
			String last_amount_value_sc = dao.getValueByCol(
					Constant.TABLE_BUS_ORDER_INFO_RESULT, rowkey_amount_last_sc,
					"order_amount");
			String last_amount_value_zy = dao.getValueByCol(
					Constant.TABLE_BUS_ORDER_INFO_RESULT, rowkey_amount_last_zy,
					"order_amount");
//			String last_amount_col_data_value = dao.getValue(
//					TableContants.TABLE_BUS_ORDER_INFO_RESULT,
//					rowkey_amount_col_data);
			String last_amount_col_data_value_sc = dao.getValue(
					Constant.TABLE_BUS_ORDER_INFO_RESULT,
					rowkey_amount_col_data_sc);
			String last_amount_col_data_value_zy = dao.getValue(
					Constant.TABLE_BUS_ORDER_INFO_RESULT,
					rowkey_amount_col_data_zy);
//			if (!"".equals(last_amount_value)) {
//				orderAmountTotal = Double.parseDouble(last_amount_value);
//			}
			if (!"".equals(last_amount_value_sc)) {
				orderAmountScTotal = Double.parseDouble(last_amount_value_sc);
			}
			if (!"".equals(last_amount_value_zy)) {
				orderAmountZyTotal = Double.parseDouble(last_amount_value_zy);
			}
//			if (!"".equals(last_amount_col_data_value)) {
//				sFinal = last_amount_col_data_value;
//			}
			if (!"".equals(last_amount_col_data_value_sc)) {
				sFinalSc = last_amount_col_data_value_sc;
			}
			if (!"".equals(last_amount_col_data_value_zy)) {
				sFinalZy = last_amount_col_data_value_zy;
			}

			/**
			 * 订单数
			 */
//			rowkey_ordr_num_last = "last_ordr_num_" + curTime;
			rowkey_ordr_num_last_sc = "last_ordr_num_sc_" + curTime;
			rowkey_ordr_num_last_zy = "last_ordr_num_zy_" + curTime;
//			rowkey_ordr_num_col_data = "col_data_ordr_num_" + curTime;
//			/** 读取Hbase获取 */
			String last_ordr_num_value_sc = dao.getValueByCol(
					Constant.TABLE_BUS_ORDER_INFO_RESULT,
					rowkey_ordr_num_last_sc, "order_num");
			String last_ordr_num_value_zy = dao.getValueByCol(
					Constant.TABLE_BUS_ORDER_INFO_RESULT,
					rowkey_ordr_num_last_zy, "order_num");
//			String last_ordr_num_col_data_value = dao.getValue(
//					TableContants.TABLE_BUS_ORDER_INFO_RESULT,
//					rowkey_ordr_num_col_data);
			if (!"".equals(last_ordr_num_value_sc)) {
				orderNumTotalSc = Long.parseLong(last_ordr_num_value_sc);
			}
			if (!"".equals(last_ordr_num_value_zy)) {
				orderNumTotalZy = Long.parseLong(last_ordr_num_value_zy);
			}
//			if (!"".equals(last_ordr_num_col_data_value)) {
//				sFinalOrdrNum = last_ordr_num_col_data_value;
//			}
			
			/**
			 * 这里补充:如果在sFinal中找不到当前小时的字符串,则进行appendNextHourStr的操作。由于重启过程中,跨小时了,这个时候curHour ==
			 * orderCreateHour,进行replaceNowHourStr操作,不会在当前小时的柱子上进行添加,所以需要补充一个小时的柱子
			 */
//			if (sFinal.indexOf("[" + curHour + ",") == -1) {
//				sFinal = CommonUtil.appendNextHourStr(curHour, sFinal, "0.0");
//				sFinalOrdrNum = CommonUtil.appendNextHourStr(curHour,
//						sFinalOrdrNum, "0");
//			}
			if (sFinalSc.indexOf("[" + curHour + ",") == -1) {
				sFinalSc = CommonUtil.appendNextHourStr(curHour, sFinalSc, "0.0");
//				sFinalOrdrNum = CommonUtil.appendNextHourStr(curHour,
//						sFinalOrdrNum, "0");
			}
			if (sFinalZy.indexOf("[" + curHour + ",") == -1) {
				sFinalZy = CommonUtil.appendNextHourStr(curHour, sFinalZy, "0.0");
//				sFinalOrdrNum = CommonUtil.appendNextHourStr(curHour,
//						sFinalOrdrNum, "0");
			}
			
			/** 获取柱状图最后一个小时的数值用来作为初始值 */
//			orderAmountHourTotal = Double.parseDouble(sFinal.substring(sFinal.lastIndexOf(",")+1,sFinal.length()-2));
			orderAmountHourScTotal = Double.parseDouble(sFinalSc.substring(sFinalSc.lastIndexOf(",")+1,sFinalSc.length()-2));
			orderAmountHourZyTotal = Double.parseDouble(sFinalZy.substring(sFinalZy.lastIndexOf(",")+1,sFinalZy.length()-2));
//			orderNumHourTotal = Long.parseLong(sFinalOrdrNum.substring(sFinalOrdrNum.lastIndexOf(",")+1,sFinalOrdrNum.length()-2));

			logger.info("===curHour===" + curHour);
			logger.info("sc amount init value is:   " + orderAmountHourScTotal);
			logger.info("zy amount init value is:   " + orderAmountHourZyTotal);
			logger.info("sc col data init value is:   " + sFinalSc);
			logger.info("zy col data init value is:   " + sFinalZy);
			logger.info("sc ordr num init value is:   " + orderNumTotalSc);
			logger.info("zy ordr num init value is:   " + orderNumTotalZy);
//			logger.info("col data ordr num init value is:   " + sFinalOrdrNum);

			dBConnection = new OracleConnection();
			con = dBConnection.getConnection() ;
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
