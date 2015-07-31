package com.yihaodian.bi.storm.business.so.catgNew;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.CommonUtil;
import com.yihaodian.bi.storm.common.util.Constant;

public class CtgPutHBaseBolt implements IBasicBolt {

	/**
	 * by 吕鹏 2014-7-4
	 * 统计品类相关指标
	 * 
	 * end_user_id , product_id 入库  ，可多并发
	 */
	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory.getLogger(CtgPutHBaseBolt.class);

	@Override
	public void cleanup() {
	}
	
	private GosOrderDao dao;
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		JumpMQOrderVo jumpMQOrderVo = (JumpMQOrderVo)input.getValue(0);
		
		try {
			if(jumpMQOrderVo==null) 
				throw new Exception("jumpMQOrderVo is null"); 
			// 订单天数，以及反序
			String orderDate=DateUtil.getFmtDate(jumpMQOrderVo.getOrderCreateTime(),DateUtil.YYYYMMDD_STR);
			if(CommonUtil.isPayOnline(jumpMQOrderVo.getPayServiceType())){//在线支付的取支付时间
				orderDate=DateUtil.getFmtDate(jumpMQOrderVo.getOrderPaymentConfirmDate(),DateUtil.YYYYMMDD_STR);
			}
			String rowKeyReverse = StringUtils.reverse(orderDate);
			
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
						String ctgID = this.getCtgIDFromProd(dao, soItemVo.getProductId()+"");
						//写db
						dao.insertRecord(Constant.TABLE_CTG_CUSTOMER_TMP, rowKeyReverse+"_"+ctgID+"_"+jumpMQOrderVo.getEndUserId(), "cf", "end_user_id",ctgID+"_"+jumpMQOrderVo.getEndUserId());
						dao.insertRecord(Constant.TABLE_CTG_SKU_TMP, rowKeyReverse+"_"+ctgID+"_"+soItemVo.getProductId(), "cf", "sku",ctgID+"_"+soItemVo.getProductId());
					}
				}
			}else {
				List<JumpMQOrderItemVo> soItemList = jumpMQOrderVo.getSoItemList() ;
				for(JumpMQOrderItemVo soItemVo : soItemList)
				{
					String ctgID = this.getCtgIDFromProd(dao, soItemVo.getProductId()+"");

					dao.insertRecord(Constant.TABLE_CTG_CUSTOMER_TMP, rowKeyReverse+"_"+ctgID+"_"+jumpMQOrderVo.getEndUserId(), "cf", "end_user_id",ctgID+"_"+jumpMQOrderVo.getEndUserId());
					dao.insertRecord(Constant.TABLE_CTG_SKU_TMP, rowKeyReverse+"_"+ctgID+"_"+soItemVo.getProductId(), "cf", "sku",ctgID+"_"+soItemVo.getProductId());
				}
			}
			collector.emit(new Values(orderDate+"_"+jumpMQOrderVo.getId()));
		}catch (Exception e) {
			logger.error("execute  error ---------------------------------"+e.getClass());
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context){
		// TODO Auto-generated method stub
		try {
			dao = new GosOrderDaoImpl();
			
		} catch (Exception e) {
			logger.error("prepare", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("orderId"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String getCtgIDFromProd(GosOrderDao dao,String product_id)
	{
		String ctgId = null;
		try {
			Result result = dao.getOneRecord("prod_ctgLv1", product_id);
			for (KeyValue keyValue : result.raw()) {
				if ("categ_lvl1_id".equals(new String(keyValue.getQualifier()))) {
					ctgId = new String(keyValue.getValue()) ;
					break;
				}
			}
		} catch (IOException e) {
			logger.error("getCtgIDFromProd", e);
		}
		return ctgId;
	}

}
