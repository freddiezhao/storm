package com.yihaodian.bi.storm.business.mobile.so;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
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
import com.yihaodian.bi.storm.business.SoHandler;
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.BusinessLogic;
import com.yihaodian.bi.storm.common.util.Get;
import com.yihaodian.bi.storm.common.util.CommonUtil;
import com.yihaodian.bi.storm.common.util.Constant;

/**
 *  手机报表 净额和品类净额
 */

public class MobileSaleBolt extends SoHandler implements IRichBolt {
	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory.getLogger(MobileSaleBolt.class);


	
	DBConnection dBConnection;
	Connection con;
	private GosOrderDao dao;
	private static long beginTime = System.currentTimeMillis();
	private static long endTime = 0;

	String[] strLastXValue;
	OutputCollector collector;
	String messageString;
	
	Date curDate; //用于跨天判断
	String curDateFmt;
	Date msgDate;
	Map<String, Double> countMap ;
	Map<String, Double> countCtgMap;
	List<JumpMQOrderItemVo> soItemList;
	int sign;
	
	@Override
	public void execute(Tuple input) {
		
		JumpMQOrderVo jumpMQOrderVo = (JumpMQOrderVo)input.getValue(0);
		
		try {
			// 判断跨天
			msgDate = jumpMQOrderVo.getMsgSendTime();
			if (msgDate.getDate() != curDate.getDate()) {
				if (msgDate.after(curDate)) {
					curDate = msgDate;
					curDateFmt = DateUtil.getFmtDate(curDate, DateUtil.YYYYMMDD_STR);
					countMap.clear();
					countCtgMap.clear();
				}
				else {
					throw new IllegalArgumentException("Message invalid. NOT CREATED TODAY. " + jumpMQOrderVo.baseInfo());
				}
			}
		}
		catch (Exception e){
			logger.warn(e.getMessage());
			return;
		}
	
//		//  成交为正，取消为负
//		logger.info("From component: `" + input.getSourceComponent() + "`");
//		if (Constant.SPOUT_ORDERCANCEL.equals(input.getSourceComponent())) {
//			sign = -1;
//		}
//		else {
//			sign = 1;
//		}
		
		sign = 1;
		
		processSoItem(input, jumpMQOrderVo);
		
		endTime = System.currentTimeMillis();
		if (endTime - beginTime >= Constant.SECOND_50)
		{
			insertData(countMap, dao);
			insertData(countCtgMap, dao);
			insertOracleSale(con,Constant.TABLE_RPT_SALE, countMap);
			insertOracleSaleInCateg(con,Constant.TABLE_RPT_SALE_CTG,countCtgMap);
			//更新开始时间
			beginTime = System.currentTimeMillis();
		}
		
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
		
		String pltfm_lvl1_id = order.getPlatformId().toString();
		String biz_unit = Get.getBizUnitByMrchtId(dao, soItem.getMerchantId()+"");
		String ctgId = Get.getCtgIDFromProd(dao, soItem.getProductId()+"");
		if (ctgId == null) logger.info("Product Id `" + soItem.getProductId() + "` did not get category Id");	
		
		String keyCtg = curDateFmt+"_ctg_" + biz_unit+"_" + ctgId + "_" + pltfm_lvl1_id;
		Double eachAmt = countCtgMap.get(keyCtg);//20140709_ctg_1_-13_100
		if (eachAmt == null) {
			eachAmt = 0.0;
		}
		if(soItem.getPromotionAmount() == null)
		{
			soItem.setPromotionAmount(new BigDecimal(0.0));
		}
		if(soItem.getCouponAmount() == null)
		{
			soItem.setCouponAmount(new BigDecimal(0.0));
		}
		if (soItem.getOrderItemAmount() == null) {
			soItem.setOrderItemAmount(new BigDecimal(0.0));
		}
		eachAmt = eachAmt + sign*(soItem.getOrderItemAmount().doubleValue()- 
				soItem.getPromotionAmount().doubleValue() - soItem.getCouponAmount().doubleValue());
		countCtgMap.put(keyCtg, eachAmt);
		
		//按天、biz_unit  统计金额
		String key = curDateFmt + "_all_" + biz_unit + "_" + pltfm_lvl1_id;
		Double amtDouble = countMap.get(key);//20140709_all_1_100_2
		if (amtDouble == null) {
			amtDouble = 0.0;
		}
		
		amtDouble = amtDouble + sign*(soItem.getOrderItemAmount().doubleValue() - 
				soItem.getPromotionAmount().doubleValue() - soItem.getCouponAmount().doubleValue());
		countMap.put(key, amtDouble);
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		try {
			dao = new GosOrderDaoImpl();
			
			this.collector = collector ;
			
			curDate = DateUtil.getDate();
			curDateFmt = DateUtil.getFmtDate(curDate, DateUtil.YYYYMMDD_STR);
			countMap = init(curDateFmt +"_all_", dao) ;
			countCtgMap = init(curDateFmt +"_ctg_", dao) ;
			
			dBConnection = new OracleConnection();
			con = dBConnection.getConnection() ;
		} catch (Exception e) {
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
	
	public void insertData(Map<String, Double> amountMap, GosOrderDao dao){
		if(dao==null){return ;}
		Iterator<String> iterator = amountMap.keySet().iterator();
		while(iterator.hasNext()) {
			String key = iterator.next() ;
			try {
				logger.info("key:" + key);
				dao.insertRecord(Constant.HBASE_RPT_AMT+ "_test", key, "cf", "order_amount", CommonUtil.getFmatDouble(amountMap.get(key)).toString());
			} catch (Exception e) {
				logger.error("insert hbase " + Constant.HBASE_RPT_AMT+ "_test" + "error." + e.getMessage());
			}
		}
	}
	
	public Map<String, Double> init(String rowKey, GosOrderDao dao) throws Exception{
		Map<String, Double> map = new HashMap<String, Double>();
		List<Result> rsList = dao.getRecordByRowKeyRegex(Constant.HBASE_RPT_AMT+ "_test", rowKey) ;
		for (Result result : rsList) {
			String rowKeyString = new String(result.getRow());
			for (KeyValue keyValue : result.raw()) {
				if ("order_amount".equals(new String(keyValue.getQualifier()))) {
					map.put(rowKeyString, Double.parseDouble(new String(keyValue.getValue()))) ;
					break;
				}
			}
		}
		return map;
	}
	
	/**
	 * Insert order amount into database regardless of categories.
	 * 
	 * @param con
	 * @param countMap
	 * @throws SQLException 
	 */
	public void insertOracleSale(Connection con, String tableName, Map<String, Double> countMap)
	{
		
		Statement statement = null;
		try {
		    statement = con.createStatement();
			Iterator<String> iterator = countMap.keySet().iterator();
			while(iterator.hasNext()) {
				String key = iterator.next() ;//20140709_all_1_100
				if (key.split("_").length != 4) {
					continue ;
				}
				String[] fields = key.split("_");
				String date_time_id = DateUtil.getFmtDate(null, DateUtil.YYYY_MM_DD_HH_MM_STR);
				
				String biz_unit = fields[2] ;
				if (biz_unit==null || "null".equals(biz_unit)) {
					continue;
				}
				String pltfm_lvl1_id = fields[3];
			    if (pltfm_lvl1_id == null || "null".equals(pltfm_lvl1_id)) {
			    	continue;
			    }
				
				String sql 
				  = "insert into " + tableName + " (DATE_TIME_ID, BIZ_UNIT, PLTFM_LVL1_ID, PRMTN_RULE_TYPE, ORDR_AMT) " +
				  		"values (to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss'),'"+biz_unit+"','" + pltfm_lvl1_id + "','" + "-99" + "','" + countMap.get(key)+"')";
			    logger.info(sql);
			    
				statement.execute(sql);
			} 
		}catch (SQLException e) {
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
	 * Insert order amount in categories into database.
	 * 
	 * @param con
	 * @param countCtgMap
	 * @throws SQLException 
	 */
	public void insertOracleSaleInCateg(Connection con, String tableName, Map<String, Double> countCtgMap)
	{
		Statement statement = null;
		Iterator<String> itor = countCtgMap.keySet().iterator();
		try {
			statement = con.createStatement();
			while(itor.hasNext()) {
				String key = itor.next() ;//  20140709_ctg_1_-13_200
				String[] fields = key.split("_");
				if (fields.length != 5) {
					continue ;
				}
				String date_time_id = DateUtil.getFmtDate(null, DateUtil.YYYY_MM_DD_HH_MM_STR);
				String biz_unit = fields[2] ;
				
				if (biz_unit==null || "null".equals(biz_unit)) {
					continue;
				}
				String ctgid = fields[3] ;
				if (ctgid==null) {
					continue;
				}
				else if ("null".equals(ctgid)) {
					ctgid = "-99999999";
				}
				String pltfm_lvl1_id = fields[4];
			    if (pltfm_lvl1_id == null || "null".equals(pltfm_lvl1_id)) {
			    	continue;
			    }
	
				String sql 
				  = "insert into " + tableName + " (DATE_TIME_ID, BIZ_UNIT, PLTFM_LVL1_ID, PRMTN_RULE_TYPE,CATEG_LVL1_ID, ORDR_AMT) " +
				  	"values (to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss'),'"+biz_unit+"','" 
					+ pltfm_lvl1_id + "','" + "-99" + "','" + ctgid + "','" + countCtgMap.get(key)+"')";
				statement.execute(sql);
			} 
		}catch (SQLException e) {
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
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	

	
	public static void main(String[] args) {
		Date curDate = DateUtil.getDate();
		String curDateFmt = DateUtil.getFmtDate(curDate, DateUtil.YYYYMMDD_STR);
		System.out.println(curDateFmt);
	}

	@Override
	public Object handleOrder(Tuple tuple, JumpMQOrderVo parentOrder,
			JumpMQOrderVo order) {
		// TODO Auto-generated method stub
		return null;
	}

}
