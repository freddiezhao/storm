package com.yihaodian.bi.storm.business.so.promotion;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.util.Get;
import com.yihaodian.bi.storm.common.util.CommonUtil;
import com.yihaodian.bi.storm.common.util.Constant;
import com.yihaodian.bi.storm.common.util.Constant;

/**
 * 闪购日累计金额
 * 
 * @author hujun1
 *
 */
public class MpOrderBolt implements IRichBolt {

	private static final long serialVersionUID = -5802174939192584380L;
	private static final Logger logger = LoggerFactory.getLogger(MpOrderBolt.class);
	
	DBConnection dBConnection = null;
	Connection con = null;
	private static GosOrderDao dao;
	private static long beginTime = System.currentTimeMillis();
	private static long endTime = 0;
	private static Map<String, Double> countMap = null;
	private static Map<String, Double> countCtgMap = null;
	private OutputCollector collector = null;
	String messageString = null;
	
	private String currDate = null; //用于跨天判断
	
	@Override
	public void cleanup() {
	}
	
	@Override
	public void execute(Tuple input) {
		JumpMQOrderItemVo soItemVo = (JumpMQOrderItemVo)input.getValueByField("item");
		String orderDate = DateUtil.getCountDate(input.getStringByField("date"), DateUtil.YYYYMMDD_STR);
		
		try {
			if(soItemVo == null) 
				throw new Exception("jumpMQOrderItemVo is null"); 
			
			if (!currDate.equals(orderDate) &&  //若订单日期不等于当前日期，并且为当前日期的后一天
					DateUtil.getDate(orderDate, DateUtil.YYYYMMDD_STR).after(DateUtil.getDate(currDate, DateUtil.YYYYMMDD_STR))) {
				insertData(countMap, dao);
				insertData(countCtgMap, dao);
				insertOracleOrder(con,countMap); 
				insertOracleOrderInCateg(con, countCtgMap);
				currDate = orderDate ;  //更新当前日期
				countMap.clear() ;
				countCtgMap.clear() ;
			}else if (! currDate.equals(orderDate) ) {
				logger.info("Unknown order date: " + orderDate ) ;
				throw new Exception("Unknown jumpMQOrderItemVo"); 
			}
			
			String uid = soItemVo.getEndUserId().toString();
			String prodID = soItemVo.getProductId().toString();
			if (isDetailGrpon(orderDate, uid, prodID)) { //查看该soItem是否为闪购小口径
				
				//闪购品类日累计金额
	//			String biz_unit = CommonOper.getBizUnitByMrchtId(dao, item.getMerchantId()+"");  //temporarily not used
				String ctgId = Get.getCtgIDFromProd(dao, soItemVo.getProductId().toString());
				if (ctgId == null)  ctgId = "-999999"; 
				String keyCtg = orderDate + "_mp_ctg_detail_"  + ctgId; //20140709_mp_ctg_detail_-13
				Double eachAmt = countCtgMap.get(keyCtg);
				if (eachAmt == null) {
					eachAmt = 0.0;
				}
				if(soItemVo.getPromotionAmount() == null)
				{
					soItemVo.setPromotionAmount(new BigDecimal(0.0));
				}
				if(soItemVo.getCouponAmount() == null)
				{
					soItemVo.setCouponAmount(new BigDecimal(0.0));
				}
				if (soItemVo.getOrderItemAmount() == null) {
					soItemVo.setOrderItemAmount(new BigDecimal(0.0));
				}
				eachAmt = eachAmt + (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
				countCtgMap.put(keyCtg, eachAmt);
					
				//闪购日累计总金额
				String key = orderDate + "_mp_all_detail";
				Double amtDouble = countMap.get(key);//20140709_mp_all_broad
				if (amtDouble == null) {
					amtDouble = 0.0;
				}
				amtDouble = amtDouble + (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
				countMap.put(key, amtDouble);
			} 
			//闪购大口径
			String ctgId = Get.getCtgIDFromProd(dao, soItemVo.getProductId().toString());
			if (ctgId == null)  ctgId = "-999999"; 
			String keyCtg = orderDate + "_mp_ctg_broad_"  + ctgId; //20140709_grpon_ctg_broad_-13
			Double eachAmt = countCtgMap.get(keyCtg);
			if (eachAmt == null) {
				eachAmt = 0.0;
			}
			if(soItemVo.getPromotionAmount() == null)
			{
				soItemVo.setPromotionAmount(new BigDecimal(0.0));
			}
			if(soItemVo.getCouponAmount() == null)
			{
				soItemVo.setCouponAmount(new BigDecimal(0.0));
			}
			if (soItemVo.getOrderItemAmount() == null) {
				soItemVo.setOrderItemAmount(new BigDecimal(0.0));
			}
			eachAmt = eachAmt + (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
			countCtgMap.put(keyCtg, eachAmt);
				
			//团购日累计总金额
			String key = orderDate + "_mp_all_broad";
			Double amtDouble = countMap.get(key);//20140709_grpon_all_broad
			if (amtDouble == null) {
				amtDouble = 0.0;
			}
			amtDouble = amtDouble + (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
			countMap.put(key, amtDouble);
			
			endTime = System.currentTimeMillis();
			if (endTime - beginTime >= Constant.SECOND_50)
			{
				try {
					insertData(countMap, dao);
					insertData(countCtgMap, dao);
					insertOracleOrder(con,countMap);
					insertOracleOrderInCateg(con, countCtgMap);
				} catch (Exception e) {
					e.printStackTrace();
				}
				//更新开始时间
				beginTime = System.currentTimeMillis();
			}
		} catch (Exception e1) {
			pl("error.MpOrderBolt:"+e1.getMessage());
		}
		
		collector.ack(input);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		try {
			dao = new GosOrderDaoImpl();
			
			this.collector = collector ;
			
			currDate = DateUtil.getFmtDate(null,DateUtil.YYYYMMDD_STR);
			
			countMap = initCountMap(Constant.TABLE_GROUPON_MP_AMT, currDate + "_mp_all", dao) ;
			countCtgMap = initCountMap(Constant.TABLE_GROUPON_MP_AMT, currDate + "_mp_ctg", dao) ;
			
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
	
	private void pl(Object o){
		System.out.println(o);
		logger.info(o.toString());
	}
	
	public void insertData(Map<String, Double> regnAmountMap, GosOrderDao dao){
		if(dao==null){return ;}
		Iterator<String> iterator = regnAmountMap.keySet().iterator();
		while(iterator.hasNext()) {
			String key = iterator.next() ;
			try {
				dao.insertRecord(Constant.TABLE_GROUPON_MP_AMT, key, Constant.COMMON_FAMILY, "order_amount", CommonUtil.getFmatDouble(regnAmountMap.get(key)).toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param dao
	 * @return
	 * @throws Exception
	 */
	public Map<String, Double> initCountMap(String tableName, String rowKey, GosOrderDao dao) throws Exception{
		if (rowKey == null) return null;
		
		Map<String, Double> map = new HashMap<String, Double>();
		List<Result> rsList = dao.getRecordByRowKeyRegex(tableName, rowKey) ;
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
	 */
	public void insertOracleOrder(Connection con,Map<String, Double> countMap)
	{
		Statement statement = null;
		try {
			statement = con.createStatement();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		
		try {
			Iterator<String> iterator = countMap.keySet().iterator();
			while(iterator.hasNext()) {
				String key = iterator.next() ;//20140709_grpon_all_broad
				String[] fields = key.split("_");
				
				if (fields.length != 4) {
					continue ;
				}
				String detailFlag = fields[3].equals("detail") ? "1" : "0";
				
				String date_time_id = DateUtil.getFmtDate(null, DateUtil.YYYY_MM_DD_HH_MM_STR);
				
				String sql 
				  = "insert into edw1_dev.rpt_grpon_mp_amt(DATE_TIME_ID, PRMTN_RULE_TYPE, ORDR_AMT, DETL_FLAG) " +
				  		"values (to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi')" + ",'7','" + countMap.get(key)+"','" + detailFlag + "')";
//				logger.info(sql);
				try {	
					statement.execute(sql);
				} catch (Exception e) {
					logger.info("error: insert rpt_grpon_mp_amt error ! ");
				}
			}
			
		} catch (Exception e) {
			logger.info("error: insert rpt_grpon_mp_amt  error ! ");
		} finally {
			try {
				statement.close() ;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Insert order amount in categories into database.
	 * 
	 * @param con
	 * @param countCtgMap
	 */
	public void insertOracleOrderInCateg(Connection con, Map<String, Double> countCtgMap)
	{
		Statement statement = null;
		try {
			statement = con.createStatement();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		
		try {
			Iterator<String> itor = countCtgMap.keySet().iterator();
			while(itor.hasNext()) {
				String key = itor.next() ;//  20140709_groupon_ctg_detail_-13
				String[] fields = key.split("_");
				if (fields.length != 5) {
					continue ;
				}
				String date_time_id = DateUtil.getFmtDate(null, DateUtil.YYYY_MM_DD_HH_MM_STR);
				String detailFlag = fields[3].equals("detail") ? "1" : "0";
				String ctgid = fields[4];
				
				String sql 
				  = "insert into edw1_dev.rpt_grpon_mp_categ_amt(DATE_TIME_ID, PRMTN_RULE_TYPE, CATEG_LVL1_ID, ORDR_AMT, DETL_FLAG) " +
				  		"values (to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi')" + ",'7','" + ctgid + "','" + countCtgMap.get(key)+"','" + detailFlag + "')";
//				logger.info(sql);
				try {	
					statement.execute(sql);
				} catch (Exception e) {
					logger.info("error: insert rpt_grpon_mp_amt error ! ");
				}
			}
			
		} catch (Exception e) {
			logger.info("error: insert rpt_grpon_mp_amt  error ! ");
		} finally {
			try {
				statement.close() ;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static boolean isDetailGrpon(String date, String uid, String prodID) throws IOException{
		String today = DateUtil.transferDateToStringCal(DateUtil.YYYYMMDD_STR, 0);
		String yesterday = DateUtil.transferDateToStringCal(DateUtil.YYYYMMDD_STR, -1);
		String theDayBefore = DateUtil.transferDateToStringCal(DateUtil.YYYYMMDD_STR, -2);
		
		if (dao.exists(Constant.TABLE_MP_USER, today + "_" + uid +  "_" + prodID) ||
				dao.exists(Constant.TABLE_MP_USER, yesterday + "_" + uid + "_" + prodID) ||
				dao.exists(Constant.TABLE_MP_USER, theDayBefore + "_" + uid + "_" + prodID))
			return true;
		
		return false;
	}
}
