package com.yihaodian.bi.storm.business.so.catgNew;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.CommonUtil;
import com.yihaodian.bi.storm.common.util.Constant;
import com.yihaodian.bi.storm.common.util.Constant;

public class CtgSoRsltBolt implements IBasicBolt {

    /**
     * by 吕鹏 2014-7-4 统计品类金额和订单数，更新db，写db点是每分钟一次，写last是5s一次 单线程执行
     */
    private static final long   serialVersionUID = 1L;

    private static final Logger logger           = LoggerFactory.getLogger(CtgSoRsltBolt.class);

    @Override
    public void cleanup() {
    }

    private GosOrderDao   dao;
    private static long   beginTime          = System.currentTimeMillis();
    private static long   endTime            = 0;
    private static String orderDate          = null;

    String                curDate            = null;                      // 用于跨天判断
    Map<String, Double>   countMap           = null;
    Map<String, Integer>  orderNumMap        = null;
    private static long   insertDB_startTime = System.currentTimeMillis();
    private static long   insertDB_endTime   = 0;

    HashSet<String>       orderNumSet        = new HashSet<String>();     // 保存20140640_-13 ，表示该品类订单需要加1

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        JumpMQOrderVo jumpMQOrderVo = (JumpMQOrderVo) input.getValue(0);

        try {
            if (jumpMQOrderVo == null) throw new Exception("jumpMQOrderVo is null");
            // 订单天数，以及反序
            orderDate = DateUtil.getFmtDate(jumpMQOrderVo.getOrderCreateTime(), DateUtil.YYYYMMDD_STR);
            if (CommonUtil.isPayOnline(jumpMQOrderVo.getPayServiceType())) {// 在线支付的取支付时间
                orderDate = DateUtil.getFmtDate(jumpMQOrderVo.getOrderPaymentConfirmDate(), DateUtil.YYYYMMDD_STR);
            }
            if (!curDate.equals(orderDate)
                && DateUtil.getDate(orderDate, DateUtil.YYYYMMDD_STR).after(DateUtil.getDate(curDate, DateUtil.YYYYMMDD_STR))) {
                // 跨天
                curDate = orderDate; // 更新当前日期
                countMap.clear();
                orderNumMap.clear();
                orderNumSet.clear();
            } else if (!curDate.equals(orderDate)) {
                logger.info(jumpMQOrderVo.getOrderCreateTime()
                            + "   orderDate is yestoday ??????????         ===========" + orderDate);
                throw new Exception("jumpMQOrderVo is yestoday  so =============");
            }

            // 判断是否有子单,如果有子单,soItem在子单中，记录产品和金额
            if (jumpMQOrderVo.getChildOrderList() != null && jumpMQOrderVo.getChildOrderList().size() > 0) {
                for (JumpMQOrderVo childOrder : jumpMQOrderVo.getChildOrderList()) {
                    List<JumpMQOrderItemVo> soItemList = childOrder.getSoItemList();
                    for (JumpMQOrderItemVo soItemVo : soItemList) {
                        if (soItemVo.getIsItemLeaf() != 1) {
                            continue;
                        }
                        String ctgID = this.getCtgIDFromProd(dao, soItemVo.getProductId() + "");
                        // 累加金额
                        Double eachAmt = countMap.get(orderDate + "_last_" + ctgID);
                        if (eachAmt == null) {
                            eachAmt = 0.0;
                        }
                        if (soItemVo.getPromotionAmount() == null) {
                            soItemVo.setPromotionAmount(new BigDecimal(0.0));
                        }
                        if (soItemVo.getCouponAmount() == null) {
                            soItemVo.setCouponAmount(new BigDecimal(0.0));
                        }
                        if (soItemVo.getOrderItemAmount() == null) {
                            soItemVo.setOrderItemAmount(new BigDecimal(0.0));
                        }
                        // eachAmt = eachAmt + (soItemVo.getOrderItemAmount().doubleValue() ) ;
                        eachAmt = eachAmt
                                  + (soItemVo.getOrderItemAmount().doubleValue()
                                     - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
                        countMap.put(orderDate + "_last_" + ctgID, eachAmt);

                        orderNumSet.add(orderDate + "_last_" + ctgID);
                    }
                }
            } else {
                List<JumpMQOrderItemVo> soItemList = jumpMQOrderVo.getSoItemList();
                for (JumpMQOrderItemVo soItemVo : soItemList) {
                    String ctgID = this.getCtgIDFromProd(dao, soItemVo.getProductId() + "");
                    // 累加金额
                    Double eachAmt = countMap.get(orderDate + "_last_" + ctgID);
                    if (eachAmt == null) {
                        eachAmt = 0.0;
                    }
                    if (soItemVo.getPromotionAmount() == null) {
                        soItemVo.setPromotionAmount(new BigDecimal(0.0));
                    }
                    if (soItemVo.getCouponAmount() == null) {
                        soItemVo.setCouponAmount(new BigDecimal(0.0));
                    }
                    if (soItemVo.getOrderItemAmount() == null) {
                        soItemVo.setOrderItemAmount(new BigDecimal(0.0));
                    }
                    // eachAmt = eachAmt + (soItemVo.getOrderItemAmount().doubleValue() ) ;
                    eachAmt = eachAmt
                              + (soItemVo.getOrderItemAmount().doubleValue()
                                 - soItemVo.getPromotionAmount().doubleValue() - soItemVo.getCouponAmount().doubleValue());
                    countMap.put(orderDate + "_last_" + ctgID, eachAmt);

                    orderNumSet.add(orderDate + "_last_" + ctgID);
                }
            }
            logger.info("orderNumSet  -- size=" + orderNumSet.size());
            for (String str : orderNumSet) {
                logger.info(orderNumSet.size() + "  orderNumSet -------------------------- " + str);
                // 订单数更新
                Integer ctgOrderNum = orderNumMap.get(str);
                if (ctgOrderNum == null) {
                    ctgOrderNum = 0;
                }
                ctgOrderNum++;
                orderNumMap.put(str, ctgOrderNum);
            }
            orderNumSet.clear();

            endTime = System.currentTimeMillis();
            synchronized (this) {
                if (endTime - beginTime >= Constant.SECOND_30 / 6) {
                    try {
                        insertData(countMap, dao, "Y");
                        insertCtgOrderNum(orderNumMap, dao, "Y");
                    } catch (Exception e) {
                        logger.error("execute -- insert 1", e);
                    }
                    // 更新开始时间
                    beginTime = System.currentTimeMillis();
                }
            }
            // 一分钟写一次db点
            insertDB_endTime = System.currentTimeMillis();
            synchronized (this) {
                if (insertDB_endTime - insertDB_startTime >= Constant.SECOND_50) {
                    try {
                        insertData(countMap, dao, "N");
                        insertCtgOrderNum(orderNumMap, dao, "N");
                    } catch (Exception e) {
                        logger.error("execute -- insert 2 ", e);
                    }
                    insertDB_startTime = System.currentTimeMillis();
                }
            }

        } catch (Exception e1) {
            logger.error("execute -- last", e1);
        }

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        // TODO Auto-generated method stub
        try {
            dao = new GosOrderDaoImpl();
            curDate = DateUtil.getFmtDate(null, DateUtil.YYYYMMDD_STR);

            countMap = initCountMap(curDate + "_last_", dao);
            orderNumMap = initCtgOrderNum(curDate + "_last_", dao);

            for (String key : countMap.keySet()) {
                logger.info("countMap init ------" + key + ":" + countMap.get(key));
            }
            for (String key : orderNumMap.keySet()) {
                logger.info("orderNumMap init ------" + key + ":" + orderNumMap.get(key));
            }

        } catch (Exception e) {
            logger.error("prepare", e);
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

    // isLastest N 为每分钟写点作为历史点。Y 为5s 更新
    public void insertData(Map<String, Double> regnAmountMap, GosOrderDao dao, String isLastest) {
        if (dao == null) {
            return;
        }
        Iterator<String> iterator = regnAmountMap.keySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();// 格式如：20140703_last_-13
            if (key == null || key.split("_").length != 3) {
                return;
            }
            String rowKey = null;
            if ("Y".equals(isLastest)) {
                rowKey = key;// 格式如：20140703_last_-13
            } else {
                rowKey = DateUtil.getLastStepMinute(0) + "_" + key.split("_")[2];// 格式如：201407041451_-13
            }
            try {
                dao.insertRecord(Constant.TABLE_CTG_RSLT_NEW, rowKey, "cf", "order_amount",
                                 CommonUtil.getFmatDouble(regnAmountMap.get(key)) + "");
            } catch (Exception e) {
                logger.error("insertData", e);
            }
        }
    }

    public Map<String, Double> initCountMap(String rowKey, GosOrderDao dao) throws Exception {
        Map<String, Double> map = new HashMap<String, Double>();
        List<Result> rsList = dao.getRecordByRowKeyRegex(Constant.TABLE_CTG_RSLT_NEW, rowKey);
        for (Result result : rsList) {
            String rowKeyString = new String(result.getRow());
            for (KeyValue keyValue : result.raw()) {
                if ("order_amount".equals(new String(keyValue.getQualifier()))) {
                    map.put(rowKeyString, Double.parseDouble(new String(keyValue.getValue())));
                    break;
                }
            }
        }
        return map;

    }

    public void insertCtgOrderNum(Map<String, Integer> regnAmountMap, GosOrderDao dao, String isLastest) {
        if (dao == null) {
            return;
        }
        Iterator<String> iterator = regnAmountMap.keySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            if (key == null || key.split("_").length != 3) {
                return;
            }
            String rowKey = null;
            if ("Y".equals(isLastest)) {
                rowKey = key;// 格式如：20140704_last_-13
            } else {
                rowKey = DateUtil.getLastStepMinute(0) + "_" + key.split("_")[2];// 格式如：201407041451_-13
            }
            try {
                dao.insertRecord(Constant.TABLE_CTG_RSLT_NEW, rowKey, "cf", "order_num", regnAmountMap.get(key)
                                                                                              + "");
            } catch (Exception e) {
                logger.error("insertCtgOrderNum", e);
            }
        }
    }

    public Map<String, Integer> initCtgOrderNum(String rowKey, GosOrderDao dao) throws Exception {
        Map<String, Integer> map = new HashMap<String, Integer>();
        List<Result> rsList = dao.getRecordByRowKeyRegex(Constant.TABLE_CTG_RSLT_NEW, rowKey);
        for (Result result : rsList) {
            String rowKeyString = new String(result.getRow());
            for (KeyValue keyValue : result.raw()) {
                if ("order_num".equals(new String(keyValue.getQualifier()))) {
                    map.put(rowKeyString, Integer.parseInt((new String(keyValue.getValue()))));
                    break;
                }
            }
        }
        return map;
    }

    public String getCtgIDFromProd(GosOrderDao dao, String product_id) {
        String ctgId = null;
        try {
            Result result = dao.getOneRecord("prod_ctgLv1", product_id);
            for (KeyValue keyValue : result.raw()) {
                if ("categ_lvl1_id".equals(new String(keyValue.getQualifier()))) {
                    ctgId = new String(keyValue.getValue());
                    break;
                }
            }
        } catch (IOException e) {
            logger.error("getCtgIDFromProd", e);
        }
        return ctgId;
    }
}
