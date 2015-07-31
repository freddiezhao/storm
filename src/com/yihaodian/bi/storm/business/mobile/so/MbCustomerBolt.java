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
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.BusinessLogic;
import com.yihaodian.bi.storm.common.util.Get;

public class MbCustomerBolt implements IRichBolt {

	/**
	 * by lvpeng 2014-7-9
	 * 推送数据到手机端
	 * 
	 * 顾客id和一级品类顾客
	 */
	private static final long serialVersionUID = 4496482586609047580L;
	private static final Logger logger = LoggerFactory.getLogger(MbCustomerBolt.class);
	private static final Integer ONLINE_PAY = 1;
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		try {
			con.close() ;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	private GosOrderDao dao;
	JumpMQOrderVo jumpMQOrderVo = null ;
	JumpMQOrderItemVo itemVo = null;
	List<JumpMQOrderItemVo> soItemList = new ArrayList<JumpMQOrderItemVo>();
	
	DBConnection dBConnection = null;
	Connection con = null;
	
	@Override
	public void execute(Tuple input) {
		jumpMQOrderVo = (JumpMQOrderVo)input.getValue(0);
		
		Date date = BusinessLogic.orderDate(jumpMQOrderVo);
		String date_id = DateUtil.getFmtDate(date, DateUtil.YYYY_MM_DD_STR);
		String date_time_id = DateUtil.getFmtDate(date, DateUtil.YYYY_MM_DD_HH_MM_STR);
		String end_user_id = jumpMQOrderVo.getEndUserId().toString();
		String order_id = jumpMQOrderVo.getId().toString();
		String biz_unit = null;
		String pltfm_lvl1_id = jumpMQOrderVo.getPlatformId().toString();
		String prmtn_rule_type = null;
		String ctgID = null;
		Long boughtTimes = jumpMQOrderVo.getBoughtTimes();
		if (boughtTimes == null) {
			boughtTimes = 0L ;
		}
		if (1 != boughtTimes) {
			boughtTimes = 0L ;
		}
		//判断是否有子单,如果有子单,soItem在子单中，记录产品和金额
		if (jumpMQOrderVo.getChildOrderList()!=null && jumpMQOrderVo.getChildOrderList().size()>0) {
			for(JumpMQOrderVo childOrder:jumpMQOrderVo.getChildOrderList())
			{
				List<JumpMQOrderItemVo> soItemList = childOrder.getSoItemList() ;
				for(JumpMQOrderItemVo soItemVo : soItemList)
				{
					if (soItemVo.getIsItemLeaf() != 1) {
						continue ;
					}
					if (soItemVo.getOrderItemAmount().equals(new BigDecimal(0.0)) && soItemVo.getIntegral().equals(new BigDecimal(0.0))) {
						continue ;
					}
					biz_unit = Get.getBizUnitByMrchtId(dao, soItemVo.getMerchantId()+"");
					if (biz_unit == null) {
						biz_unit = "-1";
						logger.warn("`BIZ_UNIT` not found. {mrchnt_id:" + soItemVo.getMerchantId() + "}");
					}
					ctgID = Get.getCtgIDFromProd(dao, soItemVo.getProductId()+"");
					if (ctgID == null) {
						ctgID = "-999999";
						logger.warn("`CATEG_LVL1_ID` not found. {prod_id:" + soItemVo.getProductId() + "}");
					}
					prmtn_rule_type = soItemVo.getPromotionRuleType().toString();
					
					// all
					this.insertOracleCustomer(con, date_id, biz_unit, pltfm_lvl1_id, prmtn_rule_type, date_time_id, end_user_id, boughtTimes);
					this.insertOracleOrder(con, date_id, biz_unit, pltfm_lvl1_id, prmtn_rule_type, date_time_id, order_id);
					//品类
					this.insertOracleCtgCustomer(con, date_id, biz_unit, pltfm_lvl1_id, prmtn_rule_type, ctgID, date_time_id, end_user_id, boughtTimes);
					this.insertOracleCtgOrder(con, date_id, biz_unit, pltfm_lvl1_id, prmtn_rule_type, ctgID, date_time_id,  order_id);
						
				}
			}
		}else {
			List<JumpMQOrderItemVo> soItemList = jumpMQOrderVo.getSoItemList() ;
			for(JumpMQOrderItemVo soItemVo : soItemList)
			{
				if (soItemVo.getOrderItemAmount().equals(new BigDecimal(0.0)) && soItemVo.getIntegral().equals(new BigDecimal(0.0))) {
					continue ;
				}
				biz_unit = Get.getBizUnitByMrchtId(dao, soItemVo.getMerchantId()+"");
				if (biz_unit == null) {
					biz_unit = "-1";
					logger.warn("`BIZ_UNIT` not found. {mrchnt_id:" + soItemVo.getMerchantId() + "}");
				}
				ctgID = Get.getCtgIDFromProd(dao, soItemVo.getProductId()+"");
				if (ctgID == null) {
					ctgID = "-999999";
					logger.warn("`CATEG_LVL1_ID` not found. {prod_id:" + soItemVo.getProductId() + "}");
				}
				prmtn_rule_type = soItemVo.getPromotionRuleType().toString();
				
				// all
				this.insertOracleCustomer(con, date_id, biz_unit, pltfm_lvl1_id, prmtn_rule_type, date_time_id, end_user_id, boughtTimes);
				this.insertOracleOrder(con, date_id, biz_unit, pltfm_lvl1_id, prmtn_rule_type, date_time_id, order_id);
				//品类
				this.insertOracleCtgCustomer(con, date_id, biz_unit, pltfm_lvl1_id, prmtn_rule_type, ctgID, date_time_id, end_user_id, boughtTimes);
				this.insertOracleCtgOrder(con, date_id, biz_unit, pltfm_lvl1_id, prmtn_rule_type, ctgID, date_time_id,  order_id);
			}
		}	
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		try {
			dao = new GosOrderDaoImpl();
			
			dBConnection = new OracleConnection();
			con = dBConnection.getConnection() ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
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
	 * 
	 * @param con
	 * @param date_id
	 * @param biz_unit
	 * @param pltfm_lvl1_id
	 * @param prmtn_rule_type
	 * @param date_time_id
	 * @param end_user_id
	 * @param boughtTimes
	 */
	public void insertOracleCustomer(Connection con,
			                         String date_id,
			                         String biz_unit,
			                         String pltfm_lvl1_id, 
			                         String prmtn_rule_type,
			                         String date_time_id,
			                         String end_user_id,
			                         Long   boughtTimes) {
		Statement statement = null ;
		try {
		    statement = con.createStatement();
		    String sql =
				"insert into rpt.rpt_realtime_mp_user_s(DATE_ID, BIZ_UNIT, PLTFM_LVL1_ID, PRMTN_RULE_TYPE, DATE_TIME_ID, END_USER_ID, NEW_CUST_FLAG) " +
				"values (to_date('"+date_id+"','yyyy-MM-dd'),'"+biz_unit+"','" + pltfm_lvl1_id + "','" + "-99" + "', to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi'),'"+end_user_id+ "','" + boughtTimes + "')";
		    logger.info(sql);
			try {
				statement.execute(sql);
			} catch (Exception e) {
				String s = "{date_id:" + date_id + ", biz_unit:" + biz_unit + ", pltfm_lvl1_id:" + pltfm_lvl1_id + ", prmtn_rule_type:" + prmtn_rule_type
						 + ", date_time_id:" + date_time_id + ", end_user_id:" + end_user_id + ", boughtTimes:" + boughtTimes 
					     + "}";
				logger.info(s + " NOT inserted into rpt.rpt_realtime_mp_user_s. "  + e.getMessage()) ;
			}
		} catch (Exception e1) {
			logger.info("Create Statement Failed before inserting rpt.rpt_realtime_mp_user_s") ;
		}finally {
			try {
				statement.close() ;
			} catch (SQLException e) {
				e.printStackTrace();
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
	 */
	public void insertOracleCtgCustomer(Connection con,
			                            String date_id,
			                            String biz_unit,
			                            String pltfm_lvl1_id,
			                            String prmtn_rule_type,
			                            String ctgId,
			                            String date_time_id,
			                            String end_user_id,
			                            Long   boughtTimes) {
		Statement statement = null ;
		try {
		    statement = con.createStatement();
		    String sql =
				"insert into rpt.rpt_realtime_mp_categ_user_s(DATE_ID, BIZ_UNIT, PLTFM_LVL1_ID, PRMTN_RULE_TYPE,CATEG_LVL1_ID, DATE_TIME_ID, END_USER_ID, NEW_CUST_FLAG) " +
				"values (to_date('"+date_id+"','yyyy-MM-dd'),'"+biz_unit+"','" + pltfm_lvl1_id + "','" + "-99" +"','"+ctgId+"', to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi'),'"+end_user_id+ "','" + boughtTimes + "')";
		    logger.info(sql);
			try {
				statement.execute(sql);
			} catch (Exception e) {
				String s = "{date_id:" + date_id + ", biz_unit:" + biz_unit + ", pltfm_lvl1_id:" + pltfm_lvl1_id + ", prmtn_rule_type:" + prmtn_rule_type
						 + ", date_time_id:" + date_time_id + ", end_user_id:" + end_user_id + ", boughtTimes:" + boughtTimes 
						 + "}";
				logger.info( s + "NOT inserted into rpt.rpt_realtime_mp_categ_user_s. " + e.getMessage()) ;
			}
		} catch (Exception e1) {
			logger.info("Create Statement Failed before inserting rpt.rpt_realtime_mp_categ_user_s") ;
		}finally
		{
			try {
				statement.close() ;
			} catch (SQLException e) {
				e.printStackTrace();
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
	 * @param date_time_id
	 * @param order_id
	 */
	public void insertOracleOrder(Connection con,
					              String date_id,
					              String biz_unit,
					              String pltfm_lvl1_id, 
					              String prmtn_rule_type,
					              String date_time_id,
					              String order_id) {
		Statement statement = null ;
		try {
		    statement = con.createStatement();
		    String sql =
				"insert into rpt.rpt_realtime_mp_ordr_s(DATE_ID, BIZ_UNIT, PLTFM_LVL1_ID, PRMTN_RULE_TYPE, DATE_TIME_ID, ORDR_ID) " +
				"values (to_date('"+date_id+"','yyyy-MM-dd'),'"+biz_unit+"','" + pltfm_lvl1_id + "','" + "-99" + "', to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi'),'"+order_id+"')";
			logger.info(sql);
			try {
				statement.execute(sql);
			} catch (Exception e) {
				String s = "{date_id:" + date_id + ", biz_unit:" + biz_unit + ", pltfm_lvl1_id:" + pltfm_lvl1_id + ", prmtn_rule_type:" + prmtn_rule_type
					    + ", date_time_id:" + date_time_id + ", order_id:" + order_id + "}";
				logger.info( s + "NOT insert into rpt.rpt_realtime_mp_ordr_s." + e.getMessage()) ;
			}
		} catch (Exception e1) {
			logger.info("Create Statement Failed before inserting rpt.rpt_realtime_mp_ordr_s err") ;
		}finally
		{
			try {
				statement.close() ;
			} catch (SQLException e) {
				e.printStackTrace();
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
	 * @param ctgid
	 * @param date_time_id
	 * @param order_id
	 */
	public void insertOracleCtgOrder(Connection con,
			                         String date_id,
			                         String biz_unit,
			                         String pltfm_lvl1_id,
			                         String prmtn_rule_type,
			                         String ctgId,
			                         String date_time_id,
			                         String order_id)
	{
		Statement statement = null ;
		try {
		    statement = con.createStatement();
		    String sql =
				"insert into rpt.rpt_realtime_mp_categ_ordr_s(DATE_ID, BIZ_UNIT, PLTFM_LVL1_ID, PRMTN_RULE_TYPE, CATEG_LVL1_ID, DATE_TIME_ID, ORDR_ID) " +
				"values (to_date('"+date_id+"','yyyy-MM-dd'),'"+biz_unit+"','" + pltfm_lvl1_id + "','" + "-99" + "','" + ctgId + "', to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi'),'" + order_id +"')";
		    logger.info(sql);
			try {
				statement.execute(sql);
			} catch (Exception e) {
				String s = "{date_id:" + date_id + ", biz_unit:" + biz_unit + ", pltfm_lvl1_id:" + pltfm_lvl1_id + ", prmtn_rule_type:" + prmtn_rule_type
						 + ", date_time_id:" + date_time_id  + ", order_id:" + order_id + "}";
				logger.info( s + "NOT inserted rpt.rpt_realtime_mp_categ_ordr_s. " + e.getMessage()) ;
			}
		} catch (Exception e1) {
			logger.info("Create Statement Failed before inserting rpt.rpt_realtime_mp_categ_ordr_s") ;
		}finally
		{
			try {
				statement.close() ;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
}
