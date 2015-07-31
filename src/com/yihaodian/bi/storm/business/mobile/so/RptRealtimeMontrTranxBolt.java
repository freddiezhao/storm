package com.yihaodian.bi.storm.business.mobile.so;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;
import com.yihaodian.bi.storm.business.SoHandler;
import com.yihaodian.bi.storm.business.so.detl.TransDetlBolt;
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.BusinessLogic;
import com.yihaodian.bi.storm.common.util.Constant;
import com.yihaodian.bi.storm.common.util.Get;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class RptRealtimeMontrTranxBolt extends SoHandler implements IRichBolt {

	private static final long serialVersionUID = 8929804069025314168L;
	private static final Logger logger = LoggerFactory.getLogger(TransDetlBolt.class);
	private GosOrderDao dao;
	private DBConnection dBConnection;
	private Connection con;
	private OutputCollector collector;
//	private JumpMQOrderVo order;
//	private JumpMQOrderVo parentOrder;
//	private JumpMQOrderItemVo soItem;
//	private Tuple tuple;
	private Map<String, String> map = new HashMap<String, String>();
	private Map<String, String> pMap = new HashMap<String, String>();

	public void execute(Tuple input) {
		JumpMQOrderVo jumpMQOrderVo = (JumpMQOrderVo) input.getValue(0);
		logger.info("Message successfully received: "+ jumpMQOrderVo.baseInfo());
		processSoItem(input, jumpMQOrderVo);
		this.collector.ack(input);
	}
	
	@SuppressWarnings("unchecked")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		try {
			dao = new GosOrderDaoImpl();
			dBConnection = new OracleConnection();
			con = dBConnection.getConnection();
			pMap = Get.getPordType();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public Object handleSoItem(Tuple tuple, JumpMQOrderVo parentOrder,
			JumpMQOrderVo order, JumpMQOrderItemVo soItem) {
//		tuple = (Tuple) o[0];
//		parentOrder = (JumpMQOrderVo) o[1];
//		order = (JumpMQOrderVo) o[2];
//		soItem = (JumpMQOrderItemVo) o[3];

		// 业务逻辑，如果金额和积分同时为零，则不算成交
		if (soItem.getOrderItemAmount().compareTo(new BigDecimal("0")) == 0
				&& soItem.getIntegral().equals(new Integer(0))) {
			return null;
		}
		// 业务逻辑 礼品卡订单过滤
		if (pMap.get(soItem.getProductId().toString()) != null) {
			return null;
		}
		// 订单时间处理
		Date date = parentOrder != null ? BusinessLogic.orderDate(parentOrder)
				: BusinessLogic.orderDate(order);
		String date_id = DateUtil.getFmtDate(date, DateUtil.YYYY_MM_DD_STR);
		String date_time_id = DateUtil.getFmtDate(date,
				DateUtil.YYYY_MM_DD_HH_MM_SS_STR);

		Long new_cust_flag = 0L;
		Long parnt_ordr_id = 0L;
		if (order.getBoughtTimes().equals(1)) {
			new_cust_flag = 1L;
		} else {
			new_cust_flag = 0L;
		}
		Long ordr_id = order.getId();
		if (order.getParentSoId() > 0) {
			parnt_ordr_id = order.getParentSoId();
		} else {
			parnt_ordr_id = order.getId();
		}
		Long city_id = order.getGoodReceiverCityId();
		Integer ordr_srce = order.getOrderSource();
		Integer sale_biz_type = order.getBizType();
		String updt_time = DateUtil.transferDateToString(order.getUpdateTime(),DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
		Long whse_id = order.getWarehouseId();
		Long end_user_id = order.getEndUserId();
		Long prod_id = soItem.getProductId();
		String categ_lvl1_id="";
		String mg_brand_id="";
		String categ_lvl2_id="";
		
		try {
		categ_lvl1_id= dao.getValueByCol("prod_info", prod_id.toString(), "categ_lvl1_id");
		mg_brand_id= dao.getValueByCol("prod_info", prod_id.toString(), "mg_brand_id");
		categ_lvl2_id= dao.getValueByCol("prod_info", prod_id.toString(), "categ_lvl2_id");
		}catch(Exception e) {
			e.printStackTrace();
		}
		//String categ_lvl1_id = Get.getCtgIDorBrandIDFromProd(dao, prod_id.toString(), "categ_lvl1_id");
		//String categ_lvl2_id = Get.getCtgIDorBrandIDFromProd(dao, prod_id.toString(), "categ_lvl2_id");
		//String mg_brand_id = Get.getCtgIDorBrandIDFromProd(dao, prod_id.toString(), "mg_brand_id");
		logger.info("categ_lvl1_id= " + categ_lvl1_id+",categ_lvl2_id="+categ_lvl2_id+",mg_brand_id="+mg_brand_id);

		String biz_unit = Get.getBizUnitByMrchtId(dao, soItem.getMerchantId()+ "");// 根据MerchntId获得biz_unit
		double pm_net_amt = 0D;

		if (soItem.getOrderItemAmount().compareTo(new BigDecimal("0")) == 0) {
			pm_net_amt = 0;
		} else {
			pm_net_amt = soItem.getOrderItemAmount().doubleValue()
					- soItem.getPromotionAmount().doubleValue()
					- soItem.getCouponAmount().doubleValue()
					- soItem.getOrderItemAmount().doubleValue()
					/ order.getOrderAmount().doubleValue()
					* order.getOrderPaidByRebate().doubleValue();
		}

		map.put("date_time_id", date_time_id);
		map.put("date_id", date_id);
		map.put("city_id", city_id.toString());
		map.put("ordr_srce", ordr_srce.toString());
		map.put("sale_biz_type", sale_biz_type.toString());
		map.put("whse_id", whse_id.toString());
		map.put("end_user_id", end_user_id.toString());
		map.put("new_cust_flag", new_cust_flag.toString());
		map.put("parnt_ordr_id", parnt_ordr_id.toString());
		map.put("updt_time", updt_time);
		map.put("ordr_id", ordr_id.toString());
		map.put("biz_unit", biz_unit);
		map.put("prod_id", prod_id.toString());
		map.put("categ_lvl1_id", categ_lvl1_id);
		map.put("categ_lvl2_id", categ_lvl2_id);
		map.put("mg_brand_id", mg_brand_id);
		map.put("pm_net_amt", String.valueOf(pm_net_amt));

		try {
			this.insertOracle(con, map);
		} catch (Exception e) {
			logger.info("INSERT FAILED " + soItem.baseInfo() + ". Caused by: "+ e.getMessage());
			this.collector.fail(tuple);
		}
		return null;
	}


	/**
	 * 将so_item 相关信息插入数据库
	 */
	public void insertOracle(Connection con, Map<String, String> map) {
		Statement statement=null;
		try {
			String sql = "insert into "
					+ Constant.TABLE_RPT_REALTEIM_MONTR_TRANX_S
					+ " ( date_id,date_time_id,city_id,ordr_srce,sale_biz_type, "
					+ "whse_id,biz_unit, categ_lvl1_id, categ_lvl2_id, mg_brand_id, prod_id, end_user_id,"
					+ "new_cust_flag,parnt_ordr_id, pm_net_amt,ordr_id,updt_time) "
					+ "values (to_date('" + map.get("date_id")
					+ "','yyyy-mm-dd')," + "to_date('"
					+ map.get("date_time_id") + "','yyyy-mm-dd hh24:mi:ss'),"
					+ map.get("city_id") + "," + map.get("ordr_srce") + ","
					+ map.get("sale_biz_type") + "," + map.get("whse_id") + ","
					+ map.get("biz_unit") + "," + map.get("categ_lvl1_id")
					+ "," + map.get("categ_lvl2_id") + ","
					+ map.get("mg_brand_id") + "," + map.get("prod_id") + ","
					+ map.get("end_user_id") + "," + map.get("new_cust_flag")
					+ "," + map.get("parnt_ordr_id") + ","
					+ map.get("pm_net_amt") + "," + map.get("ordr_id") + ","
					+ "to_date('" + map.get("updt_time")
					+ "','yyyy-mm-dd hh24:mi:ss')" + ")";
			logger.info(sql);
			statement = con.createStatement();
			statement.execute(sql);
			statement.close();
		} catch (SQLException e) {
			logger.info(e.getMessage());
			logger.info("error: insert  "+ Constant.TABLE_RPT_REALTEIM_MONTR_TRANX_S + " error ! ");
		}finally {
            try {
                statement.close();
            } catch (SQLException e) {
            	logger.error("Close Statement Exception", e);
            }
        }
		
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public Object handleOrder(Object... o) {
		return null;
	}

	public Object handleOrder(Tuple tuple, JumpMQOrderVo parentOrder,
			JumpMQOrderVo order) {
		return null;
	}

}
