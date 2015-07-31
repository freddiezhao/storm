package com.yihaodian.bi.storm.business.so.promotion;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

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
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.util.Constant;

/**
 * 团闪订单详情
 * 
 * @author hujun1
 *
 */
public class GrponMpOrdrDetlBolt implements IRichBolt {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7813847829630311872L;
	private static final Logger logger = LoggerFactory.getLogger(GrponMpOrdrDetlBolt.class);
	
	private DBConnection dBConnection = null;
	private Connection con = null;
	private OutputCollector _collector = null;
	private static final String SCHEMA = "rpt";
	private static final int GRPON = 2;
	private static final int MP = 7;
	private static final Integer UNKNOWN = -999999;
	
	@Override
	public void cleanup() {
	}
	
	@Override
	public void execute(Tuple input) {
		String streamId = input.getSourceStreamId();
		JumpMQOrderItemVo soItemVo = (JumpMQOrderItemVo)input.getValueByField("item");
		if (soItemVo == null || soItemVo.getIsItemLeaf() != 1) return;
		
		String orderDate = input.getStringByField("date");
		Long parentOrderId = input.getLongByField("orderid");
		Integer orderSource = input.getIntegerByField("ordersource");
		Long provId = input.getLongByField("provid");
		Long cityId = input.getLongByField("cityid");
		Long newCustFlag = input.getLongByField("buytimes") == 1  ? 1L : 0L;
		
		Long orderId = parentOrderId;
		if (parentOrderId <= 0)
			orderId = soItemVo.getOrderId();			
		Long prodId = soItemVo.getProductId();
		Long mrchntId = soItemVo.getMerchantId();
		Long uid = soItemVo.getEndUserId();
		Integer pmNum = soItemVo.getOrderItemNum();
		BigDecimal pmAmt = soItemVo.getOrderItemAmount();
		BigDecimal promotionAmt = soItemVo.getPromotionAmount();
		BigDecimal couponAmt = soItemVo.getCouponAmount();
		Integer ruleType = UNKNOWN;

		if (streamId.equals("grpon")) {
			ruleType = GRPON;
		}
		else if (streamId.equals("mp")) {
			ruleType = MP;
		}
		
		if (pmAmt == null) pmAmt = new BigDecimal(0.0);
		if (promotionAmt == null) promotionAmt = new BigDecimal(0.0);
		if (couponAmt == null) couponAmt = new BigDecimal(0.0);
		Double pmNetAmt = pmAmt.doubleValue() - promotionAmt.doubleValue() - couponAmt.doubleValue();
		
		orderDate = DateUtil.getCountDate(orderDate, DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
		String updateTime = DateUtil.getFmtDate(null, DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
		Long soItemId = soItemVo.getId();
		insertToOracle(con, orderDate, orderId, prodId, mrchntId, uid, pmNetAmt, pmNum, 
				ruleType, orderSource, updateTime, provId, cityId, soItemId, newCustFlag);
		
		_collector.ack(input);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector ;
		dBConnection = new OracleConnection();
		con = dBConnection.getConnection() ;
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
	
	/**
	 * 
	 * @param con
	 * @param date_time_id
	 * @param orderId
	 * @param prodId
	 * @param mrchntId
	 * @param uid
	 * @param pmNetAmt
	 * @param pmNum
	 * @param ruleType
	 * @param orderSource
	 * @param update_time
	 */
	private static void insertToOracle(Connection con, String date_time_id, Long orderId, 
			Long prodId, Long mrchntId, Long uid, Double pmNetAmt, Integer pmNum, Integer ruleType, 
			Integer orderSource, String update_time, Long provId, Long cityId, Long so_item_id, Long buyTimes){
		Statement statement = null;
		
		try {
			statement = con.createStatement();
			
			String sql = 
				"insert into " + SCHEMA + "." + Constant.TABLE_ORDER_GROUPON_MP_DETL + "(DATE_ID, DATE_TIME_ID, ORDR_ID, PROD_ID, MRCHNT_ID, END_USER_ID, PM_NET_AMT, PM_NUM, PRMTN_RULE_TYPE, ORDER_SOURCE, UPDATE_TIME, PROV_ID, CITY_ID, SO_ITEM_ID, NEW_CUST_FLAG) " +
		  		"values (trunc(to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss')), to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss'), '" +  orderId + "', '" + prodId + "', '" + mrchntId + "', '" + uid + "', '" + pmNetAmt + "', '" 
				+ pmNum + "', '" + ruleType + "', '" + orderSource + "', " + "to_date('" + update_time + "','yyyy-MM-dd hh24:mi:ss'), '" + provId + "' , '" + cityId + "' , '" + so_item_id +  "' , '" + buyTimes + "')";
			statement.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.info("Insert to Oracle failed!");
		} finally {
			try {
				statement.close() ;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
