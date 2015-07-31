package com.yihaodian.bi.storm.business.so.detl;

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
import backtype.storm.topology.base.BaseRichBolt;
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
 * 
 */

public class TransDetlBolt extends SoHandler implements IRichBolt {
	

	private static final long serialVersionUID = 8929804069025314168L;
	private static final Logger logger = LoggerFactory.getLogger(TransDetlBolt.class);
	
	private DBConnection dBConnection;
	private Connection con;
	private OutputCollector collector;
	
	@Override
	public void execute(Tuple input) {
		JumpMQOrderVo jumpMQOrderVo = (JumpMQOrderVo)input.getValue(0);
		
		processSoItem(input, jumpMQOrderVo);
		
		this.collector.ack(input);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		try {
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
	 * 处理so_item数据
	 * 
	 * @param input 
	 * @param soItemVo 
	 */
	@Override
	public Object handleSoItem(Tuple tuple, JumpMQOrderVo parentOrder,
			JumpMQOrderVo order, JumpMQOrderItemVo soItem) {
		
		Date date = parentOrder != null ? BusinessLogic.orderDate(parentOrder) : BusinessLogic.orderDate(order);
		String dateId = DateUtil.getFmtDate(date, DateUtil.YYYY_MM_DD_STR);
		String dateTimeId = DateUtil.getFmtDate(date, DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
		Long soId = order.getId();
		Long parentSoId = order.getParentSoId();
		BigDecimal rebateAmt = order.getOrderPaidByRebate();
		String updateTime = DateUtil.getFmtDate(null, DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
		
		if (soItem.getOrderItemAmount().compareTo(new BigDecimal("0")) == 0 && soItem.getIntegral().equals(new Integer(0))) {
			return null;
		}
		
//			logger.info("So item : " + soItemVo.baseInfo()); 
		this.insertTrans(con, dateId, dateTimeId, soId, parentSoId, soItem.getId(), 
			soItem.getParentSoItemId(), soItem.getProductId(), soItem.getOrderItemAmount(),soItem.getOrderItemNum(), 
			soItem.getPromotionAmount(), soItem.getCouponAmount(), rebateAmt, updateTime, soItem.getIntegral());
		
		return null;
	}
	
	/**
	 * 将so_item 相关信息插入数据库
	 * 
	 * @param con
	 * @param dateId
	 * @param dateTimeId
	 * @param soId
	 * @param parentSoId
	 * @param soItemId
	 * @param parentSoItemId
	 * @param productId
	 * @param orderItemAmount
	 * @param orderItemNum
	 * @param promotionAmount
	 * @param couponAmount
	 * @param rebateAmt
	 * @param updateTime
	 * @throws SQLException
	 */
	private void insertTrans(Connection con, String dateId, String dateTimeId,
			Long soId, Long parentSoId, Long soItemId, Long parentSoItemId,
			Long productId, BigDecimal orderItemAmount, Integer orderItemNum,
			BigDecimal promotionAmount, BigDecimal couponAmount,
			BigDecimal rebateAmt, String updateTime, Integer integral) {
		
		String sql =
				"insert into " + Constant.TABLE_FCT_TRANS_DETL + " (DATE_ID, DATE_TIME_ID, SO_ID, P_SO_ID, SO_ITEM_ID, P_SO_ITEM_ID,"
						+ "PROD_ID, ITEM_AMT, ITEM_NUM, PRMT_AMT, COUP_AMT, REBT_AMT, UPDATE_TIME, INTEGRAL) " +
				"values (to_date('"+dateId+"','yyyy-MM-dd') , to_date('"+ dateTimeId + "','yyyy-MM-dd hh24:mi:ss'),'"
				+ soId + "','" + parentSoId + "','" + soItemId + "','" + parentSoItemId + "','" + productId + "','"
				+ orderItemAmount + "','" + orderItemNum + "','" + promotionAmount + "','" + couponAmount + "','" 
				+ rebateAmt + "', to_date('"+updateTime+"','yyyy-MM-dd hh24:mi:ss')" + ", '" + integral+ "')";
		logger.info(sql);
		Statement statement = null;
		try {
			statement  = con.createStatement();
			statement.execute(sql);
		} catch (SQLException e) {
			logger.info("INSERT FAILED. Caused by: ",  e);
		} finally {
			try {
				statement.close();
			} catch (SQLException e) {
				logger.error("Statement not closed. Caused by: ",  e);
			}
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object handleOrder(Tuple tuple, JumpMQOrderVo parentOrder,
			JumpMQOrderVo order) {
		// TODO Auto-generated method stub
		return null;
	}
}