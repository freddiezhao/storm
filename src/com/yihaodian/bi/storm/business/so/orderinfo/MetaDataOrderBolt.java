package com.yihaodian.bi.storm.business.so.orderinfo;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.CommonUtil;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * @author zhangsheng2
 * 元数据入oracle
 */
public class MetaDataOrderBolt implements IRichBolt{

	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory
	.getLogger(MetaDataOrderBolt.class);
	
	DBConnection dBConnection = null;
	
	Connection con = null;
	
	OutputCollector _collector;
	
	JumpMQOrderVo jumpMQOrderVo = null;

	List<JumpMQOrderItemVo> soItemList = new ArrayList<JumpMQOrderItemVo>();

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		try {
			jumpMQOrderVo = (JumpMQOrderVo) input.getValue(0);
			if (jumpMQOrderVo == null) {
				logger.info("===元数据空消息体===");
				throw new Exception();
			}
			//入库父单
			try {
				insertParentSo(con,jumpMQOrderVo);
			} catch (Exception e) {
				logger.info("===so insert failed ....===");
			}
			
			
			//入库子单
			if (jumpMQOrderVo.getChildOrderList()!=null && jumpMQOrderVo.getChildOrderList().size()>0) {
				for(JumpMQOrderVo childOrder:jumpMQOrderVo.getChildOrderList())
				{
					soItemList = childOrder.getSoItemList() ;
					for(JumpMQOrderItemVo soItemVo : soItemList)
					{
						insertItemSo(con,soItemVo);
					}
				}
			}else {
				soItemList = jumpMQOrderVo.getSoItemList() ;
				for(JumpMQOrderItemVo soItemVo : soItemList)
				{
					insertItemSo(con,soItemVo);
				}
			}
			_collector.ack(input);
			
		}catch(Exception e){
			_collector.fail(input);
			e.printStackTrace();
		}finally{}
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		try {
		this._collector = collector;
		dBConnection = new OracleConnection();
		con = dBConnection.getConnection() ;
		}catch(Exception e){
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
	 * 插入父单信息
	 */
	private void insertParentSo(Connection con,JumpMQOrderVo jumpMQOrderVo){
		PreparedStatement st = null;
		String sql = "insert into edw1_dev.REALTIME_METADATA_SO (ID, ENDUSERID, ORDERCODE, ORDERCREATETIME, ORDERTYPE, ORDERSTATUS, BIZTYPE, ORDERSOURCE, PARENTSOID, SITETYPE, ORDERAMOUNT, PRODUCTAMOUNT, ISLEAF, PAYSERVICETYPE, MSGSENDTIME, UPDT_TIME) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		try {
			st = con.prepareStatement(sql);
			st.setLong(1, jumpMQOrderVo.getId());
			st.setLong(2,jumpMQOrderVo.getEndUserId());
			st.setString(3, jumpMQOrderVo.getOrderCode());
//			st.setDate(4, new java.sql.Date(jumpMQOrderVo.getOrderCreateTime().getTime()));
			if(jumpMQOrderVo.getOrderCreateTime()!=null){
				st.setTimestamp(4, new Timestamp(jumpMQOrderVo.getOrderCreateTime().getTime()));
			}else{
				st.setTimestamp(4, new Timestamp(new Date().getTime()));
			}
			st.setInt(5, jumpMQOrderVo.getOrderType());
			st.setInt(6, jumpMQOrderVo.getOrderStatus());
			st.setInt(7, jumpMQOrderVo.getBizType());
			st.setInt(8, jumpMQOrderVo.getOrderSource());
			st.setLong(9,jumpMQOrderVo.getParentSoId());
			st.setInt(10, jumpMQOrderVo.getSiteType());
			if(jumpMQOrderVo.getOrderAmount()!=null){
				st.setDouble(11, jumpMQOrderVo.getOrderAmount().doubleValue());
			}else{
				st.setDouble(11, 0.0);
			}
			if(jumpMQOrderVo.getProductAmount()!=null){
				st.setDouble(12, jumpMQOrderVo.getProductAmount().doubleValue());
			}else{
				st.setDouble(12, 0.0);
			}
			st.setInt(13, jumpMQOrderVo.getIsLeaf());
			st.setInt(14, jumpMQOrderVo.getPayServiceType());
//			st.setDate(15, new java.sql.Date(jumpMQOrderVo.getMsgSendTime().getTime()));
			if(jumpMQOrderVo.getMsgSendTime()!=null){
				st.setTimestamp(15, new Timestamp(jumpMQOrderVo.getMsgSendTime().getTime()));
			}else{
				st.setTimestamp(15, new Timestamp(new Date().getTime()));
			}
			st.setTimestamp(16, new Timestamp(new Date().getTime()));
			st.executeQuery();
		} catch (Exception e) {
			logger.info("error: Metadata insert error ! ");
		} finally {
			try {
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 插入子单信息
	 */
	private void insertItemSo(Connection con,JumpMQOrderItemVo soItemVo){
		PreparedStatement st = null;
		String sql = "insert into edw1_dev.REALTIME_METADATA_SO_ITEM (ID, ENDUSERID, ORDERID, PRODUCTID, MERCHANTID, ORDERITEMAMOUNT, ORDERITEMPRICE, ORDERITEMNUM, PARENTSOITEMID, ISITEMLEAF, DELIVERYFEEAMOUNT, PROMOTIONAMOUNT, COUPONAMOUNT, CREATETIME, RULE_TYPE, UPDT_TIME) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		try {
			st = con.prepareStatement(sql);
			st.setLong(1, soItemVo.getId());
			st.setLong(2,soItemVo.getEndUserId());
			st.setLong(3, soItemVo.getOrderId());
			st.setLong(4, soItemVo.getProductId());
			st.setLong(5, soItemVo.getMerchantId());
			if(soItemVo.getOrderItemAmount()!=null){
				st.setDouble(6, soItemVo.getOrderItemAmount().doubleValue());
			}else{
				st.setDouble(6, 0.0);
			}
			if(soItemVo.getOrderItemPrice()!=null){
				st.setDouble(7, soItemVo.getOrderItemPrice().doubleValue());
			}else{
				st.setDouble(7, 0.0);
			}
			st.setInt(8, soItemVo.getOrderItemNum());
			st.setLong(9,soItemVo.getParentSoItemId());
			st.setInt(10, soItemVo.getIsItemLeaf());
			if(soItemVo.getDeliveryFeeAmount()!=null){
				st.setDouble(11, soItemVo.getDeliveryFeeAmount().doubleValue());
			}else{
				st.setDouble(11, 0.0);
			}
			if(soItemVo.getPromotionAmount()!=null){
				st.setDouble(12, soItemVo.getPromotionAmount().doubleValue());
			}else{
				st.setDouble(12, 0.0);
			}
			if(soItemVo.getCouponAmount()!=null){
				st.setDouble(13, soItemVo.getCouponAmount().doubleValue());
			}else{
				st.setDouble(13, 0.0);
			}
			if(soItemVo.getCreateTime()!=null){
				st.setTimestamp(14, new Timestamp(soItemVo.getCreateTime().getTime()));
			}else{
				st.setTimestamp(14, new Timestamp(new Date().getTime()));
			}
			st.setInt(15, soItemVo.getRuleType());
			st.setTimestamp(16, new Timestamp(new Date().getTime()));
			st.executeQuery();
		} catch (Exception e) {
			logger.info("error: Metadata item insert error ! ");
		} finally {
			try {
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
