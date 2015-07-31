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
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.BusinessLogic;
import com.yihaodian.bi.storm.common.util.Get;
import com.yihaodian.bi.storm.common.util.CommonUtil;
import com.yihaodian.bi.storm.common.util.Constant;

public class MbOrderBolt implements IRichBolt {

	/**
	 * by lvpeng 2014-7-9
	 * 推送数据到手机端
	 * 
	 * 金额 和 品类金额
	 */
	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory.getLogger(MbOrderBolt.class);


	@Override
	public void cleanup() {
	}
	
	DBConnection dBConnection = null;
	Connection con = null;
	private GosOrderDao dao;
	private static long beginTime = System.currentTimeMillis();
	private static long endTime = 0;

	String[] strLastXValue=null;
	OutputCollector collector = null;
	String messageString = null;
	
	Date curDate; //用于跨天判断
	Date msgDate;
	Map<String, Double> countMap = null;
	
	Map<String, Double> countCtgMap = null;
	
	@Override
	public void execute(Tuple input) {
		JumpMQOrderVo jumpMQOrderVo = (JumpMQOrderVo)input.getValue(0);
		
		try {
			
			// 判断跨天
			msgDate = jumpMQOrderVo.getMsgSendTime();
			if (msgDate.getDate() != curDate.getDate()) {
				if (msgDate.after(curDate)) {
					curDate = msgDate ; 
					countMap.clear() ;
					countCtgMap.clear() ;
				}
				else {
					throw new IllegalArgumentException("Message invalid. NOT CREATED TODAY");
				}
			}
			
			
			String orderDate = DateUtil.getFmtDate(curDate, DateUtil.YYYYMMDD_STR);
			String pltfm_lvl1_id = jumpMQOrderVo.getPlatformId().toString();

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
						String biz_unit = Get.getBizUnitByMrchtId(dao, soItemVo.getMerchantId()+"");
						String ctgId = Get.getCtgIDFromProd(dao, soItemVo.getProductId()+"");
						if (ctgId == null) logger.info("Product Id `" + soItemVo.getProductId() + "` did not get category Id");	
						
						String keyCtg = orderDate+"_ctg_" + biz_unit+"_" + ctgId + "_" + pltfm_lvl1_id;
						Double eachAmt = countCtgMap.get(keyCtg);//20140709_ctg_1_-13_100
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
						
						//按天、biz_unit  统计金额
						String key = orderDate + "_all_" + biz_unit + "_" + pltfm_lvl1_id;
						Double amtDouble = countMap.get(key);//20140709_all_1_100_2
						if (amtDouble == null) {
							amtDouble = 0.0;
						}
						
						amtDouble = amtDouble + (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
						countMap.put(key, amtDouble);
						
					}
				}
			}else {
				List<JumpMQOrderItemVo> soItemList = jumpMQOrderVo.getSoItemList() ;
				for(JumpMQOrderItemVo soItemVo : soItemList)
				{
					String biz_unit = Get.getBizUnitByMrchtId(dao, soItemVo.getMerchantId()+"");
					String ctgId = Get.getCtgIDFromProd(dao, soItemVo.getProductId()+"");
					
					String keyCtg = orderDate+"_ctg_" + biz_unit + "_" + ctgId + "_" + pltfm_lvl1_id;
					Double eachAmt = countCtgMap.get(keyCtg); //20140709_ctg_1_-13_200
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
					
					//按天、biz_unit  统计金额
					String key = orderDate + "_all_" + biz_unit + "_" + pltfm_lvl1_id;
					Double amtDouble = countMap.get(key);//20140709_all_1_200
					if (amtDouble == null) {
						amtDouble = 0.0;
					}
					
					amtDouble = amtDouble + (soItemVo.getOrderItemAmount().doubleValue() - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
					countMap.put(key, amtDouble);
				}
			}
			
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
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		try {
			dao = new GosOrderDaoImpl();
			
			this.collector = collector ;
			
			curDate = DateUtil.getDate();
			String curDateFmt = DateUtil.getFmtDate(curDate, DateUtil.YYYYMMDD_STR);
			countMap = initCountMap(curDateFmt +"_all_", dao) ;
			countCtgMap = initCountMap(curDateFmt +"_ctg_", dao) ;
			
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
	
	public void insertData(Map<String, Double> regnAmountMap, GosOrderDao dao){
		if(dao==null){return ;}
		Iterator<String> iterator = regnAmountMap.keySet().iterator();
		while(iterator.hasNext()) {
			String key = iterator.next() ;
			try {
				dao.insertRecord("rpt_realtime_mp_amt_test", key, "cf", "order_amount", CommonUtil.getFmatDouble(regnAmountMap.get(key)) + "");
			} catch (Exception e) {
				logger.error("insert hbase error. `rpt_realtime_mp_amt_test`.");
				e.printStackTrace();
			}
		}
	}
	public Map<String, Double> initCountMap(String rowKey, GosOrderDao dao) throws Exception{
		Map<String, Double> map = new HashMap<String, Double>();
		List<Result> rsList = dao.getRecordByRowKeyRegex("rpt_realtime_mp_amt_test", rowKey) ;
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
				  = "insert into rpt.rpt_realtime_mp_amt_s(DATE_TIME_ID, BIZ_UNIT, PLTFM_LVL1_ID, PRMTN_RULE_TYPE, ORDR_AMT) " +
				  		"values (to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss'),'"+biz_unit+"','" + pltfm_lvl1_id + "','" + "-99" + "','" + countMap.get(key)+"')";
				//logger.info(sql);
				try {
					statement.execute(sql);
				} catch (SQLException e) {
					logger.info("insert rpt.rpt_realtime_mp_amt_s failed ! " + e.getMessage());
				}
			}
		} catch (Exception e){
			logger.error("insert rpt.rpt_realtime_mp_amt_s error !" + e.getMessage());
		}finally {
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
				  = "insert into rpt.rpt_realtime_mp_categ_amt_s(DATE_TIME_ID, BIZ_UNIT, PLTFM_LVL1_ID, PRMTN_RULE_TYPE,CATEG_LVL1_ID, ORDR_AMT) " +
				  		"values (to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss'),'"+biz_unit+"','" + pltfm_lvl1_id + "','" + "-99" + "','" + ctgid + "','" + countCtgMap.get(key)+"')";
				//logger.info(sql);
				try {
					statement.execute(sql);
				} catch (SQLException e) {
					logger.info("insert rpt.rpt_realtime_mp_categ_amt_s failed ! " + e.getMessage());
			    }	
			} 
		} catch (Exception e){
			logger.error("insert rpt.rpt_realtime_mp_categ_amt_s error !" + e.getMessage());
		} finally {
			try {
				statement.close() ;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
