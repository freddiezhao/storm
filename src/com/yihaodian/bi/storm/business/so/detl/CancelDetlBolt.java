package com.yihaodian.bi.storm.business.so.detl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.BusinessLogic;
import com.yihaodian.bi.storm.common.util.Constant;


/**
 * 
 */

public class CancelDetlBolt implements IRichBolt {
	
	private static final long serialVersionUID = 5316410356366925551L;
	private static final Logger logger = LoggerFactory.getLogger(CancelDetlBolt.class);
	
	private DBConnection dBConnection;
	private Connection con;
	private OutputCollector collector;
	
	@Override
	public void execute(Tuple input) {
		JumpMQOrderVo jumpMQOrderVo = (JumpMQOrderVo)input.getValue(0);
		
		processSo(input, jumpMQOrderVo);
		
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
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		try {
			con.close() ;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void  processSo(Tuple input, JumpMQOrderVo jumpMQOrderVo) {		
		logger.info("Message successfully received: " + jumpMQOrderVo.baseInfo());
		
		if (jumpMQOrderVo.getChildOrderList()!=null && !jumpMQOrderVo.getChildOrderList().isEmpty()) {
			for(JumpMQOrderVo childOrder:jumpMQOrderVo.getChildOrderList())
			{
				logger.info("Order successfully received: " + childOrder.baseInfo());
				
				List<JumpMQOrderItemVo> soItemList = childOrder.getSoItemList();
				
				for(JumpMQOrderItemVo soItemVo : soItemList)
				{
					if (soItemVo.getSubSoItemList() != null && !soItemVo.getSubSoItemList().isEmpty()) {
						
						List<JumpMQOrderItemVo> subSoItemList = soItemVo.getSubSoItemList();
						
						for (JumpMQOrderItemVo subSoItemVo : subSoItemList) {
							processSoItem(input, childOrder, subSoItemVo);
						}
					}
					else {
						processSoItem(input, childOrder, soItemVo);
					}
				}
				logger.info("Order successfully processed: " + childOrder.baseInfo());
			}
		}else {
			logger.info("Order successfully received: " + jumpMQOrderVo.baseInfo());
			
			List<JumpMQOrderItemVo> soItemList = jumpMQOrderVo.getSoItemList();
			
			for(JumpMQOrderItemVo soItemVo : soItemList)
			{
				if (soItemVo.getSubSoItemList() != null && !soItemVo.getSubSoItemList().isEmpty()) {
					
					List<JumpMQOrderItemVo> subSoItemList = soItemVo.getSubSoItemList();
					
					for (JumpMQOrderItemVo subSoItemVo : subSoItemList) {
						processSoItem(input, jumpMQOrderVo, subSoItemVo);
					}
					
				}
				else {
					processSoItem(input, jumpMQOrderVo, soItemVo);
				}
			}
			logger.info("Order successfully processed: " + jumpMQOrderVo.baseInfo());
		}
	}
	
	/**
	 * 处理so_item数据
	 * 
	 * @param input 
	 * @param soItemVo 
	 */
	private void processSoItem(Tuple input,JumpMQOrderVo soVo, JumpMQOrderItemVo soItemVo) {
		
		Date date = BusinessLogic.orderDate(soVo);
		String dateId = DateUtil.getFmtDate(date, DateUtil.YYYY_MM_DD_STR);
		String dateTimeId = DateUtil.getFmtDate(date, DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
		String cancelTime = DateUtil.getFmtDate(soVo.getCancelDate(), DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
		Long soId = soVo.getId();
		Long parentSoId = soVo.getParentSoId();
		BigDecimal rebateAmt = soVo.getOrderPaidByRebate();
		String updateTime = DateUtil.getFmtDate(null, DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
		
		
		if (soItemVo.getOrderItemAmount().compareTo(new BigDecimal("0")) == 0 && soItemVo.getIntegral().equals(new Integer(0))) {
			return;
		}
		try {
			insertCancel(con, dateId, dateTimeId, cancelTime, soId, parentSoId, soItemVo.getId(), 
					soItemVo.getParentSoItemId(), soItemVo.getProductId(), soItemVo.getOrderItemAmount(),soItemVo.getOrderItemNum(), 
					soItemVo.getPromotionAmount(), soItemVo.getCouponAmount(), rebateAmt, updateTime);
		} catch (SQLException e) {
			logger.info("INSERT FAILED " + soItemVo.baseInfo() +". Caused by: " + e.getMessage());
			this.collector.fail(input);
		}
	}
	
	private void insertCancel(Connection con, String dateId, String dateTimeId, String cancelTime,
			Long soId, Long parentSoId, Long soItemId, Long parentSoItemId,
			Long productId, BigDecimal orderItemAmount, Integer orderItemNum,
			BigDecimal promotionAmount, BigDecimal couponAmount,
			BigDecimal rebateAmt, String updateTime) throws SQLException {
		Statement statement = null ;
		String sql =
				"insert into " + Constant.TABLE_FCT_CANCEL_DETL + " (DATE_ID, DATE_TIME_ID, CANCEL_TIME, SO_ID, P_SO_ID, SO_ITEM_ID, P_SO_ITEM_ID,"
						+ "PROD_ID, ITEM_AMT, ITEM_NUM, PRMT_AMT, COUP_AMT, REBT_AMT, UPDATE_TIME) " +
				"values (to_date('"+dateId+"','yyyy-MM-dd') , to_date('"+ dateTimeId + "','yyyy-MM-dd hh24:mi:ss'), to_date('"+ cancelTime + "','yyyy-MM-dd hh24:mi:ss'),'"
				+ soId + "','" + parentSoId + "','" + soItemId + "','" + parentSoItemId + "','" + productId + "','"
				+ orderItemAmount + "','" + orderItemNum + "','" + promotionAmount + "','" + couponAmount + "','" 
				+ rebateAmt + "', to_date('"+updateTime+"','yyyy-MM-dd hh24:mi:ss'))";
		statement = con.createStatement();
		statement.execute(sql);
		statement.close() ;
	}
}