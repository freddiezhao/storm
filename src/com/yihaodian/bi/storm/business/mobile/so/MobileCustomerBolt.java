package com.yihaodian.bi.storm.business.mobile.so;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;
import com.yihaodian.bi.storm.business.SoHandler;
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.BusinessLogic;
import com.yihaodian.bi.storm.common.util.Constant;
import com.yihaodian.bi.storm.common.util.Get;


/**
 * 顾客id和一级品类顾客
 */
public class MobileCustomerBolt extends SoHandler implements IRichBolt {

	private static final long serialVersionUID = -3033475009366970538L;
	
	private static final Logger logger = LoggerFactory.getLogger(MobileCustomerBolt.class);
	private static final String tbName1= Constant.TABLE_RPT_CUST;
	private static final String tbName2= Constant.TABLE_RPT_CUST_CTG;
	
	private OutputCollector collector;
	
	private GosOrderDao dao;
	private JumpMQOrderVo order;
	private JumpMQOrderVo parentOrder;
	private JumpMQOrderItemVo soItem;
	private Tuple tuple;
	
	private Connection con;
	
	@Override
	public void execute(Tuple input) {
		JumpMQOrderVo o = (JumpMQOrderVo)input.getValue(0);
		
		processSoItem(input, o);
		
		this.collector.ack(input);
	}
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		try {
			con.close() ;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 处理so_item数据
	 * 
	 * @param input 
	 * @param soItemVo 
	 */
	@Override
	public Object handleSoItem(Tuple tuple, JumpMQOrderVo parentOrder,
			JumpMQOrderVo order, JumpMQOrderItemVo soItem) {
		
		// 业务逻辑，如果金额和积分同时为零，则不算成交
		if (soItem.getOrderItemAmount().compareTo(new BigDecimal("0"))==0 
				&& soItem.getIntegral().equals(new Integer(0))) {
			return null;
		}
		
		Date date = parentOrder != null ? BusinessLogic.orderDate(parentOrder) : BusinessLogic.orderDate(order);
		String dateId = DateUtil.getFmtDate(date, DateUtil.YYYY_MM_DD_STR);
		String dateTimeId = DateUtil.getFmtDate(date, DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
		String biz_unit = Get.getBizUnitByMrchtId(dao, soItem.getMerchantId()+"");
		String pltfm_lvl1_id = order.getPlatformId().toString();
		String end_user_id = order.getEndUserId().toString();
		if (biz_unit == null) {
			biz_unit = "-1";
			logger.warn("`BIZ_UNIT` not found. {mrchnt_id:" + soItem.getMerchantId() + "}");
		}
		String ctgID = Get.getCtgIDFromProd(dao, soItem.getProductId()+"");
		if (ctgID == null) {
			ctgID = "-999999";
			logger.warn("`CATEG_LVL1_ID` not found. {prod_id:" + soItem.getProductId() + "}");
		}
		String prmtn_rule_type = soItem.getPromotionRuleType().toString();
		Long boughtTimes = order.getBoughtTimes();
		
		insertOracleCustomer(con, tbName1, dateId,
          biz_unit,pltfm_lvl1_id, prmtn_rule_type,dateTimeId,end_user_id,boughtTimes);
		insertOracleCtgCustomer(con, tbName2, dateId, 
		  biz_unit, pltfm_lvl1_id, prmtn_rule_type, ctgID, dateTimeId, end_user_id, boughtTimes);
		
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		try {
			this.dao = new GosOrderDaoImpl();
			
			this.con = new OracleConnection().getConnection();
		} catch (Exception e) {
			logger.info("Connecting database failed... Caused by:" , e);
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
	
	/**
	 * 
	 * 
	 * @param con
	 * @param date_id
	 * @param biz_unit
	 * @param pltfm_lvl1_id
	 * @param prmtn_rule_type
	 * @param date_time_id
	 * @param end_user_id
	 * @param boughtTimes
	 * @throws SQLException 
	 */
	public void insertOracleCustomer(Connection con,
									 String table_name,
			                         String date_id,
			                         String biz_unit,
			                         String pltfm_lvl1_id, 
			                         String prmtn_rule_type,
			                         String date_time_id,
			                         String end_user_id,
			                         Long   boughtTimes) {
		
		    String sql =
				"insert into " + table_name + " (DATE_ID, BIZ_UNIT, PLTFM_LVL1_ID, PRMTN_RULE_TYPE, DATE_TIME_ID, END_USER_ID, NEW_CUST_FLAG) " +
				"values (to_date('"+date_id+"','yyyy-MM-dd'),'"+biz_unit+"','" + pltfm_lvl1_id + "','" + "-99" + "', to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss'),'"+end_user_id+ "','" + boughtTimes + "')";
		    logger.info(sql);
		    Statement statement = null;
			try {
				statement  = con.createStatement();
				statement.execute(sql);
			} catch (SQLException e) {
				if(e.getMessage().indexOf("unique") > -1 ){
					logger.info(e.getMessage());
				}else{
					logger.info("INSERT FAILED. Caused by: ",  e);
				}
			} finally {
				try {
					statement.close();
				} catch (SQLException e) {
					logger.error("Statement not closed. Caused by: ",  e);
				}
			}
	}
	
	/**
	 * 
	 * @param con
	 * @param date_id
	 * @param biz_unit
	 * @param pltfm_lvl1_id
	 * @param prmtn_rule_type
	 * @param ctgId
	 * @param date_time_id
	 * @param end_user_id
	 * @param boughtTimes
	 * @throws SQLException 
	 */
	public void insertOracleCtgCustomer(Connection con,
										String table_name,
			                            String date_id,
			                            String biz_unit,
			                            String pltfm_lvl1_id,
			                            String prmtn_rule_type,
			                            String ctgId,
			                            String date_time_id,
			                            String end_user_id,
			                            Long   boughtTimes) {
	    String sql =
			"insert into " + table_name + "(DATE_ID, BIZ_UNIT, PLTFM_LVL1_ID, PRMTN_RULE_TYPE,CATEG_LVL1_ID, DATE_TIME_ID, END_USER_ID, NEW_CUST_FLAG) " +
			"values (to_date('"+date_id+"','yyyy-MM-dd'),'"+biz_unit+"','" + pltfm_lvl1_id + "','" + "-99" +"','"+ctgId+"', to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss'),'"+end_user_id+ "','" + boughtTimes + "')";
	    
	    logger.info(sql);
	    Statement statement = null;
		try {
			statement  = con.createStatement();
			statement.execute(sql);
		} catch (SQLException e) {
			if(e.getMessage().indexOf("unique") > -1 ){
				logger.info(e.getMessage());
			}else{
				logger.info("INSERT FAILED. Caused by: ",  e);
			}
		} finally {
			try {
				statement.close();
			} catch (SQLException e) {
				logger.error("Statement not closed. Caused by: ",  e);
			}
		}
	}

	@Override
	public Object handleOrder(Tuple tuple, JumpMQOrderVo parentOrder,
			JumpMQOrderVo order) {
		// TODO Auto-generated method stub
		return null;
	}



}
