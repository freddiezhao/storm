package com.yihaodian.bi.storm.business.so.customer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.BusinessLogic;
import com.yihaodian.bi.storm.common.util.CommonUtil;
import com.yihaodian.bi.storm.common.util.Constant;
import com.yihaodian.bi.storm.common.util.Constant;

/**
 * 类CustomerBolt.java的实现描述：TODO 类实现描述
 */
public class CustomerBolt implements IRichBolt {

    private static final long         serialVersionUID           = 4496482586609047580L;
    private static final Logger       LOG                        = LoggerFactory.getLogger(CustomerBolt.class);

    private static final List<String> BATCHLIST_ALL_CUSTOMERS    = new ArrayList<String>();
    private static final List<String> BATCHLIST_MOBILE_CUSTOMERS = new ArrayList<String>();
    private static final List<String> BATCHLIST_YHD_CUSTOMERS    = new ArrayList<String>();
    private static final List<String> BATCHLIST_NONYHD_CUSTOMERS = new ArrayList<String>();
    private static long               TIME_BEGIN                 = System.currentTimeMillis();
    private static long               TIME_END                   = 0;
    private String                    curTimeString              = null;
    private static long               curAllCustomersCount       = 0;

    private DBConnection              conUtil                    = null;
    private Connection                con                        = null;
    private GosOrderDao               orderDAO;
    private JumpMQOrderVo             curOrderObj                = null;
    private BusinessLogic         logicUtil                  = null;

    // Double LastXValue = 0.0;
    String[]                          strLastXValue              = null;

    @Override
    public void execute(Tuple input) {
        curOrderObj = (JumpMQOrderVo) input.getValue(0); // 多线程下，input.getValue(0)的某个值不会被多个线程重复获取，即线程安全

        // 合法 Order
        if (curOrderObj != null && curOrderObj.getEndUserId() > 0) {
            computeAllCustomers(curOrderObj);
            computeMobileCustomers(curOrderObj);
            computeDifferentCustomers(curOrderObj);
        } else {
            LOG.warn("Illegal Order " + curOrderObj.toString());
        }

        // 批处理
        TIME_END = System.currentTimeMillis();
        synchronized (this) {
            if (TIME_END - TIME_BEGIN >= Constant.MINUTE_1) {
                // 计算顾客数，并写点，计算前2分钟的顾客数
                curTimeString = DateUtil.getLastStepMinute(-2);
                strLastXValue = CommonUtil.getLastXValueStr();
                batchDifferentCustomers();
                batchMobileCustomers();
                batchAllCustomers();
                // 更新开始时间
                TIME_BEGIN = System.currentTimeMillis();
            }
        }
    }

    private void batchMobileCustomers() {
        try {
            orderDAO.insertGosOrder(Constant.TABLE_MOBILE_CUSTOMER_RESULT, curTimeString, "cf", new String[] {
                    "customer_num", "xValue", "xTitle" }, new String[] { BATCHLIST_MOBILE_CUSTOMERS.size() + "",
                    strLastXValue[1], strLastXValue[0] });
            LOG.info("distinct count of mobile customers writed to HBase " + curTimeString + "----"
                     + BATCHLIST_MOBILE_CUSTOMERS.size());
        } catch (Exception e) {
            LOG.error("Computer Mobile Customers ERROR ", e);
        } finally {
            BATCHLIST_MOBILE_CUSTOMERS.clear();
        }
    }

    private void computeMobileCustomers(JumpMQOrderVo o) {
    	String curInfoString = StringUtils.reverse(DateUtil.getCurMinute(o.getOrderCreateTime())) + "_"
                + o.getEndUserId();
        if (curOrderObj.getOrderSource() == 3) {// 移动端
        	if (!BATCHLIST_MOBILE_CUSTOMERS.contains(curInfoString))
        		BATCHLIST_MOBILE_CUSTOMERS.add(curInfoString);
        }
    }

    /**
     * 批处理不同类型的顾客数 (自营，非自营)
     */
    private void batchDifferentCustomers() {
        try {
            orderDAO.insertGosOrder(Constant.TABLE_CUSTOMER_RESULT, curTimeString, "cf", new String[] {
                    "customer_yhd_num", "xValue", "xTitle" }, new String[] { BATCHLIST_YHD_CUSTOMERS.size() + "",
                    strLastXValue[1], strLastXValue[0] });
            LOG.info("distinct count of yhd customers writed to HBase " + curTimeString + "----"
                     + BATCHLIST_YHD_CUSTOMERS.size());
            orderDAO.insertGosOrder(Constant.TABLE_CUSTOMER_RESULT, curTimeString, "cf", new String[] {
                    "customer_nonyhd_num", "xValue", "xTitle" }, new String[] { BATCHLIST_NONYHD_CUSTOMERS.size() + "",
                    strLastXValue[1], strLastXValue[0] });
            LOG.info("distinct count of nonyhd customers writed to HBase " + curTimeString + "----"
                     + BATCHLIST_NONYHD_CUSTOMERS.size());
        } catch (Exception e) {
            LOG.error("Computer Diff Customers ERROR ", e);
        } finally {
            BATCHLIST_NONYHD_CUSTOMERS.clear();
            BATCHLIST_YHD_CUSTOMERS.clear();
        }
    }

    private void computeDifferentCustomers(JumpMQOrderVo o) {
        if (o.getChildOrderList() != null && o.getChildOrderList().size() > 0) {
            for (JumpMQOrderVo co : o.getChildOrderList()) {
                if (co != null) {
                    String curInfoString = StringUtils.reverse(DateUtil.getCurMinute(o.getOrderCreateTime())) + "_"
                                           + o.getEndUserId();
                    if (co.getMerchantId() == null) {
                        LOG.error("No Merchant Id");
                    } else {
                        if (logicUtil.isYHDSelf(co.getMerchantId())) {
                            if (!BATCHLIST_YHD_CUSTOMERS.contains(curInfoString)) {
                                BATCHLIST_YHD_CUSTOMERS.add(curInfoString);
                            }
                        } else {
                            if (!BATCHLIST_NONYHD_CUSTOMERS.contains(curInfoString)) {
                                BATCHLIST_NONYHD_CUSTOMERS.add(curInfoString);
                            }
                        }
                    }
                }
            }
        } else {
            String curInfoString = StringUtils.reverse(DateUtil.getCurMinute(o.getOrderCreateTime())) + "_"
                                   + o.getEndUserId();
            if (o.getMerchantId() == null) {
                LOG.error("No Merchant Id");
            } else {
                if (logicUtil.isYHDSelf(o.getMerchantId())) {
                    if (!BATCHLIST_YHD_CUSTOMERS.contains(curInfoString)) {
                        BATCHLIST_YHD_CUSTOMERS.add(curInfoString);
                    }
                } else {
                    if (!BATCHLIST_NONYHD_CUSTOMERS.contains(curInfoString)) {
                        BATCHLIST_NONYHD_CUSTOMERS.add(curInfoString);
                    }
                }
            }
        }
    }

    private void computeAllCustomers(JumpMQOrderVo o) {
        String curInfoString = StringUtils.reverse(DateUtil.getCurMinute(curOrderObj.getOrderCreateTime())) + "_"
                               + curOrderObj.getEndUserId();
        if (!BATCHLIST_ALL_CUSTOMERS.contains(curInfoString)) {
            // for computeAllCustomers
            BATCHLIST_ALL_CUSTOMERS.add(curInfoString);
        }
    }

    private void batchAllCustomers() {
        try {
            //insertOracle(con, BATCHLIST_ALL_CUSTOMERS, curTimeString);
            //LOG.info("all customers writed to Oracle = " + BATCHLIST_ALL_CUSTOMERS.size());
            // 读库, 获取顾客数
            curAllCustomersCount = BATCHLIST_ALL_CUSTOMERS.size();
            // 写点
            orderDAO.insertGosOrder(Constant.TABLE_CUSTOMER_RESULT, curTimeString, "cf", new String[] {
                    "customer_num", "xValue", "xTitle" }, new String[] { curAllCustomersCount + "", strLastXValue[1],
                    strLastXValue[0] });
            // 写入最后操作时间
            orderDAO.insertGosOrder(Constant.TABLE_DICT, Constant.ROWKEY_DICT_CUSTOMER_RSLT, "cf",
                                    Constant.COLUMN_DICT_LASTTIME, curTimeString);
            LOG.info("distinct count of all customers writed to HBase " + curTimeString + "----" + curAllCustomersCount);
        } catch (Exception e) {
            LOG.error("Computer All Customers ERROR ", e);
        } finally {
            BATCHLIST_ALL_CUSTOMERS.clear();
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            orderDAO = new GosOrderDaoImpl();
            conUtil = new OracleConnection();
            con = conUtil.getConnection();
            logicUtil = new BusinessLogic(con);
            if (!logicUtil.flushYHDSelfIds()) {
                throw new Exception("Flush YHDSelfIDs Error ! [" + logicUtil.getYHDSelfIDs() + "]");
            } else {
                LOG.info("Init YHDSelfIDs [" + logicUtil.getYHDSelfIDs() + "]");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

//    public void insertOracle(Connection con, List<String> prefix_user_id_list, String dateString) {
//        Statement statement = null;
//        try {
//            statement = con.createStatement();
//
//            for (String list : prefix_user_id_list) {
//                String arr[] = list.split("_");
//                if (arr.length < 2) {
//                    return;
//                }
//                String end_user_id = arr[1];
//                String datePrefix = StringUtils.reverse(arr[0]);
//                datePrefix = dateString;
//                Date date = DateFmat.getDate(datePrefix, DateFmat.YYYYMMDDHHMM_STR);
//                String date_id = DateFmat.getFmtDate(date, DateFmat.YYYY_MM_DD_STR);
//                String date_time_id = DateFmat.getFmtDate(date, DateFmat.YYYY_MM_DD_HH_MM_STR);
//                String sql = "insert into rpt.rpt_realtime_mp_cust(date_id,date_time_id,end_user_id) "
//                             + "values (to_date('" + date_id + "','yyyy-MM-dd'),to_date('" + date_time_id
//                             + "','yyyy-MM-dd hh24:mi'),'" + end_user_id + "')";
//                try {
//                    statement.execute(sql);
//                } catch (Exception e) {
//                    LOG.error(sql);
//                    LOG.error("insert rpt.rpt_realtime_mp_cust err !", e);
//                }
//            }
//        } catch (Exception e1) {
//            LOG.error("CustomerBolt Insert to Oracle Exception ", e1);
//        } finally {
//            try {
//                statement.close();
//            } catch (SQLException e) {
//                LOG.error("Close Statement Exception", e);
//            }
//        }
//    }

    @Override
    public void cleanup() {
        try {
            if (con != null) con.close();
        } catch (SQLException e) {
            LOG.error("Cleanup Close Connection ERROR", e);
        }
    }

}
