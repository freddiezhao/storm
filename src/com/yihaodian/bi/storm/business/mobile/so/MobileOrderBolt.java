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
 * 推送数据到手机端
 * 
 * 顾客id和一级品类顾客
 */
public class MobileOrderBolt extends SoHandler implements IRichBolt {

	private static final long serialVersionUID = -4391675173250107297L;
	private static final Logger logger = LoggerFactory.getLogger(MobileOrderBolt.class);
	
	private GosOrderDao dao;
	
	private DBConnection dBConnection;
	private Connection con;
	private OutputCollector collector;
	
	private JumpMQOrderVo order;
	private JumpMQOrderVo parentOrder;
	private JumpMQOrderItemVo soItem;
	private Tuple tuple;
	
	@Override
	public void execute(Tuple input) {
		JumpMQOrderVo o = (JumpMQOrderVo)input.getValue(0);
		
		processSoItem(input, o);
		
		this.collector.ack(input);
	}
	
	@Override
	public Object handleSoItem(Tuple tuple, JumpMQOrderVo parentOrder,
			JumpMQOrderVo order, JumpMQOrderItemVo soItem) {
		
		// 业务逻辑，如果金额和积分同时为零，则不算成交
		if (soItem.getOrderItemAmount().compareTo(new BigDecimal("0"))==0 
				&& soItem.getIntegral().equals(new Integer(0))) {
			return null;
		}
		
		Date date = parentOrder != null ? BusinessLogic.orderDate(parentOrder) : BusinessLogic.orderDate(order);
		String date_id = DateUtil.getFmtDate(date, DateUtil.YYYY_MM_DD_STR);
		String date_time_id = DateUtil.getFmtDate(date, DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
		String order_id = order.getId().toString();
		String biz_unit = null;
		String pltfm_lvl1_id = order.getPlatformId().toString();
		String prmtn_rule_type = null;
		String ctgID = null;
	
		biz_unit = Get.getBizUnitByMrchtId(dao, soItem.getMerchantId()+"");
		if (biz_unit == null) {
			biz_unit = "-1";
			logger.warn("`BIZ_UNIT` not found. {mrchnt_id:" + soItem.getMerchantId() + "}");
		}
		ctgID = Get.getCtgIDFromProd(dao, soItem.getProductId()+"");
		if (ctgID == null) {
			ctgID = "-999999";
			logger.warn("`CATEG_LVL1_ID` not found. {prod_id:" + soItem.getProductId() + "}");
		}
		prmtn_rule_type = soItem.getPromotionRuleType().toString();
		
		// all
		this.insertOracleOrder(con, Constant.TABLE_RPT_ORDR, 
				date_id, biz_unit, pltfm_lvl1_id, prmtn_rule_type, date_time_id, order_id);
		//品类
		this.insertOracleCtgOrder(con, Constant.TABLE_RPT_ORDR_CTG, 
				date_id, biz_unit, pltfm_lvl1_id, prmtn_rule_type, ctgID, date_time_id,  order_id);
				
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		try {
			this.collector = collector;
			dao = new GosOrderDaoImpl();
			
			dBConnection = new OracleConnection();
			con = dBConnection.getConnection() ;
		} catch (Exception e) {
			logger.info("Connecting database failed... Caused by:" + e.getMessage());
		}
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
	 * @param date_id
	 * @param biz_unit
	 * @param pltfm_lvl1_id
	 * @param prmtn_rule_type
	 * @param date_time_id
	 * @param order_id
	 * @throws SQLException 
	 */
	private void insertOracleOrder(Connection con,
								  String table_name,
					              String date_id,
					              String biz_unit,
					              String pltfm_lvl1_id, 
					              String prmtn_rule_type,
					              String date_time_id,
					              String order_id)  {
		    String sql =
				"insert into " + table_name + " (DATE_ID, BIZ_UNIT, PLTFM_LVL1_ID, PRMTN_RULE_TYPE, DATE_TIME_ID, ORDR_ID) " +
				"values (to_date('"+date_id+"','yyyy-MM-dd'),'"+biz_unit+"','" + pltfm_lvl1_id + "','" + "-99" 
				 + "', to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss'),'"+order_id+"')";
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
	 * @param pltfm_lvl1_idl
	 * @param prmtn_rule_type
	 * @param ctgid
	 * @param date_time_id
	 * @param order_id
	 * @throws SQLException 
	 */
	private void insertOracleCtgOrder(Connection con,
									 String table_name,
			                         String date_id,
			                         String biz_unit,
			                         String pltfm_lvl1_id,
			                         String prmtn_rule_type,
			                         String ctgId,
			                         String date_time_id,
			                         String order_id)
	{
			String sql =
				"insert into " + table_name + " (DATE_ID, BIZ_UNIT, PLTFM_LVL1_ID, PRMTN_RULE_TYPE, CATEG_LVL1_ID, DATE_TIME_ID, ORDR_ID) " +
				"values (to_date('"+date_id+"','yyyy-MM-dd'),'"+biz_unit+"','" + pltfm_lvl1_id + "','" + "-99" + "','" + ctgId 
				+ "', to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss'),'" + order_id +"')";
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
//				this.collector.fail(tuple);
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
