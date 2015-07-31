package com.yihaodian.bi.storm.business.so.orderinfo;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.google.common.util.concurrent.AtomicDouble;
import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;
import com.yihaodian.bi.storm.business.SoHandler;
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.util.BusinessLogic;
import com.yihaodian.bi.storm.common.util.CommonUtil;
import com.yihaodian.bi.storm.common.util.Constant;
import com.yihaodian.bi.storm.common.util.Constant;

/**
 * by zhangsheng 2014-5-20 统计成交订单净额 功能：获得JumpMQOrderVo成交订单信息,累加计算 update by zhangsheng 2014-6-18 去掉RocketMQ
 */
public class SalesBolt extends SoHandler implements IRichBolt {

    private static final long   serialVersionUID = 7525807650106935719L;

    private static final Logger logger              = LoggerFactory.getLogger(SalesBolt.class);
    
    private Tuple tuple;
    private JumpMQOrderVo parentOrder;
    private JumpMQOrderVo order;
    private JumpMQOrderItemVo soItem;
    private BusinessLogic util;

    @Override
    public void cleanup() {
        try {
            if (con != null) con.close();
        } catch (SQLException e) {
            logger.error("[cleanup] Close Connection Error !", e);
        }
    }

    DBConnection                dBConnection                  = null;
    Connection                  con                           = null;
    OutputCollector             _collector;

    private GosOrderDao         dao;
    JumpMQOrderVo               jumpMQOrderVo                 = null;

    private static long         insertDB_startTime            = System.currentTimeMillis();
    private static long         insertDB_endTime              = 0;

    private static long         insertDB_last_point_startTime = System.currentTimeMillis();
    private static long         insertDB_last_point_endTime   = 0;

    /** 线图-订单净额入库rowkey */
    private String              rowkey_amount                 = null;
    /** 线图-订单净额最后累加值入库rowkey 重启bolt时需要读库获取最后累加值 */
    private String              rowkey_amount_last            = null;
    /** 当天日期 */
    private String              curTime                       = null;
    /** 订单创建日期 */
    private String              orderCreateDate               = null;
    /** 订单净额累加值 */
    private static Double       orderAmountTotal              = 0.0;
    /** 坐标点信息 */
    private String[]            strLastXValue                 = null;
    /** 柱状图-柱状图展示完整数据字符串rowkey */
    private String              rowkey_amount_col_data        = null;
    /** 当前小时数 */
    private int                 curHour;
    /** 订单创建-小时数 */
    private int                 orderCreateHour;
    /** 小时-订单净额累加值 */
    private static Double       orderAmountHourTotal          = 0.0;
    /** 柱状图展示 数据字符串 完整形式 */
    private String              colData                       = null;
    /** 柱状图展示 用于拼接处理 */
    private static String       sFinal                        = "[]";

    private int                 dealHourFlg                   = 0;

    /** 线图-订单数入库rowkey */
    private String              rowkey_ordr_num               = null;
    /** 线图-订单数最后累加值入库rowkey 重启bolt时需要读库获取最后累加值 */
    private String              rowkey_ordr_num_last          = null;
    /** 订单数累加值 */
    private static long         orderNumTotal                 = 0;
    /** 小时-订单净额累加值 */
    private static long         orderNumHourTotal             = 0;

    /** 柱状图-柱状图展示完整数据字符串rowkey */
    private String              rowkey_ordr_num_col_data      = null;
    /** 柱状图展示 数据字符串 完整形式 */
    private String              colDataOrdrNum                = null;
    /** 柱状图展示 用于拼接处理 */
    private static String       sFinalOrdrNum                 = "[]";

    private static AtomicInteger discountItemTotal             = new AtomicInteger(0);

    // 总的折扣价格————无效虚拟价格总数
    private static AtomicDouble discountPriceTotal             = new AtomicDouble(0.0);
    
    // 用于跨天判断
    private Date msgDate;
	private Date curDate;
    
    @Override
    public void execute(Tuple input) {
        try {
            // 当前坐标点信息
            strLastXValue = CommonUtil.getCurFullXValueStr();
            jumpMQOrderVo = (JumpMQOrderVo) input.getValue(0);// 多线程下，input.getValue(0)的某个值不会被多个线程重复获取，即线程安全
            Double orderAmount = 0.0;
            // 获取订单创建时间
            if (jumpMQOrderVo == null) {
                logger.info("====空消息体===");
                throw new Exception();
            }
            
			// 跨天判断
			msgDate = jumpMQOrderVo.getMsgSendTime();
			if (msgDate.getDate() != curDate.getDate()) {
			  if (msgDate.after(curDate)){ // 跨天
				logger.info("===跨天处理===" + orderCreateDate);
				
				curDate = msgDate;
				
                /** 跨天处理 */
                curTime = DateUtil.getFmtDate(msgDate, DateUtil.YYYYMMDD_STR);
                curHour = msgDate.getHours() + 1;

                /** 清空小时数据 */
                orderAmountHourTotal = 0.0;
                orderNumHourTotal = 0;

                orderAmountTotal = 0.0;
                rowkey_amount_last = "last_amount_" + curTime;
                rowkey_amount_col_data = "col_data_" + curTime;
                sFinal = "[]";

                orderNumTotal = 0;
                rowkey_ordr_num_last = "last_ordr_num_" + curTime;
                rowkey_ordr_num_col_data = "col_data_ordr_num_" + curTime;
                sFinalOrdrNum = "[]";

                discountItemTotal.set(0);
                discountPriceTotal.set(0);

                dealHourFlg = 0;
			  }
			  else if (msgDate.before(curDate)) { // 非当天的历史订单
				  return;
			  }
			}
            
			// 处理订单
			orderAmount = processSoItem(input, jumpMQOrderVo);
			
            Date orderDate = BusinessLogic.orderDate(jumpMQOrderVo);
            if (orderDate == null) throw new IllegalArgumentException("Order Date is NULL");
            orderCreateDate = DateUtil.getFmtDate(orderDate, DateUtil.YYYYMMDD_STR);
            orderCreateHour = orderDate.getHours() + 1;

            
            // 进行订单净额累加操作
            logger.info("orderAmount:" + orderAmount );
            orderAmountTotal += orderAmount;
            logger.info("orderAmountTotal:" + orderAmountTotal );
            orderNumTotal++;

            // 订单创建小时数和当前系统小时数相同则进行小时订单净额累加操作,否则进行跨小时处理
            if (curHour == orderCreateHour) {
                // 如果sFinal为空,则初始化
                if ("[]".equals(sFinal)) {
                    sFinal = CommonUtil.appendNextHourStr(curHour, sFinal, "0");
                }
                orderAmountHourTotal += orderAmount;
                orderNumHourTotal++;

                // 柱状图当前小时数据更新,之后需要重新设值
                sFinal = CommonUtil.replaceNowHourStr(sFinal, orderAmountHourTotal.toString());
                colData = sFinal;

                if ("[]".equals(sFinalOrdrNum)) {
                    sFinalOrdrNum = CommonUtil.appendNextHourStr(curHour, sFinalOrdrNum, "0");
                }
                sFinalOrdrNum = CommonUtil.replaceNowHourStr(sFinalOrdrNum, String.valueOf(orderNumHourTotal));
                colDataOrdrNum = sFinalOrdrNum;
            } else if (curHour < orderCreateHour) {// 需要强制入库,否则会丢失至多30秒的数据
            	logger.info("===跨小时处理===" + orderCreateHour);
                /** 跨小时处理 */
                curHour = orderCreateHour;

                orderAmountHourTotal = 0.0;
                // 柱状图拼接新的小时数据
                sFinal = CommonUtil.appendNextHourStr(curHour, sFinal, orderAmountHourTotal.toString());
                colData = sFinal;

                orderNumHourTotal = 0;
                sFinalOrdrNum = CommonUtil.appendNextHourStr(curHour, sFinalOrdrNum,
                                                             String.valueOf(orderNumHourTotal));
                colDataOrdrNum = sFinalOrdrNum;
            } else {// 当前时间大于订单创建时间,表示mq里面有之前阻塞的订单,需要特殊处理或忽略
            	logger.info("===处理阻塞订单===订单id:" + jumpMQOrderVo.baseInfo());
            }

            /** 写habse */
            // 插入小时级最后累加值记录,30秒一次入库，如果期间有很多订单过来，30秒期间刚好碰到跨小时的情况的话，那部分数据就会丢失
            synchronized (this) {
                insertDB_last_point_endTime = System.currentTimeMillis();
                // 5秒写入一次最后的点,前端5秒读取一次刷新最新的值insertDB_last_point_startTime
                if (insertDB_last_point_endTime - insertDB_last_point_startTime >= Constant.SECOND_5) {
                    /** 订单净额 */
                	logger.info("orderAmountTotal : " + orderAmountTotal);
                	logger.info("orderNumHourTotal:"  + orderNumHourTotal);
                    // 插入最后累加值记录,重启bolt的时候需要获取最后一次插入的金额,在此基础上再进行累加
                    dao.insertGosOrder(Constant.TABLE_ORDER_INFO_RESULT, rowkey_amount_last,
                                       Constant.COMMON_FAMILY,
                                       new String[] { "order_amount", "xValue", "xTitle" }, new String[] {
                                               orderAmountTotal + "", strLastXValue[1], strLastXValue[0] });

                    // 插入拼接的柱状图数据,单条记录,后端重启和前端展示获取历史都从这里获取
                    dao.insertGosOrder(Constant.TABLE_ORDER_INFO_RESULT, rowkey_amount_col_data,
                                       Constant.COMMON_FAMILY, "col_data", colData);

                    /** 订单数 */
                    dao.insertGosOrder(Constant.TABLE_ORDER_INFO_RESULT, rowkey_ordr_num_last,
                                       Constant.COMMON_FAMILY, new String[] { "order_num", "xValue", "xTitle" },
                                       new String[] { orderNumTotal + "", strLastXValue[1], strLastXValue[0] });

                    dao.insertGosOrder(Constant.TABLE_ORDER_INFO_RESULT, rowkey_ordr_num_col_data,
                                       Constant.COMMON_FAMILY, "col_data_ordr_num", colDataOrdrNum);

                    insertDB_last_point_startTime = System.currentTimeMillis();
                }

            }

            synchronized (this) {
                insertDB_endTime = System.currentTimeMillis();
                if (insertDB_endTime - insertDB_startTime >= Constant.SECOND_50) {
                    /**
                     * 线图数据处理
                     */
                    rowkey_amount = curTime + "_amount_" + strLastXValue[0].replace(".", "");
                    // 插入点的历史数据,线图中用来拼接历史数据线条
                    dao.insertGosOrder(Constant.TABLE_ORDER_INFO_RESULT, rowkey_amount,
                                       Constant.COMMON_FAMILY,
                                       new String[] { "order_amount", "xValue", "xTitle" }, new String[] {
                                               orderAmountTotal + "", strLastXValue[1], strLastXValue[0] });

                    rowkey_ordr_num = curTime + "_ordr_num_" + strLastXValue[0].replace(".", "");
                    dao.insertGosOrder(Constant.TABLE_ORDER_INFO_RESULT, rowkey_ordr_num,
                                       Constant.COMMON_FAMILY, new String[] { "order_num", "xValue", "xTitle" },
                                       new String[] { orderNumTotal + "", strLastXValue[1], strLastXValue[0] });

                    logger.info("===50秒入库成功===");
                    insertDB_startTime = System.currentTimeMillis();

                    // 写Oracle ，推送数据到手机端报表
                    //insertOracle(con, orderAmountTotal, orderNumTotal);
                }
            }
        } catch (Exception e) {
        	logger.info("ERROR. Caused by:" , e);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
        try {
            /** 初始化以下值 */
            dao = new GosOrderDaoImpl();

            curDate = Calendar.getInstance().getTime();
            curTime = DateUtil.getFmtDate(curDate, DateUtil.YYYYMMDD_STR);
            curHour = DateUtil.getCurHour() + 1;

            rowkey_amount_last = "last_amount_" + curTime;
            rowkey_amount_col_data = "col_data_" + curTime;
            /** 读取Hbase获取 */
//            String last_amount_value = dao.getValueByCol(Constant.TABLE_ORDER_INFO_RESULT, rowkey_amount_last,
//                                                         "order_amount");
//            String last_amount_col_data_value = dao.getValue(Constant.TABLE_ORDER_INFO_RESULT,
//                                                             rowkey_amount_col_data);
            
// test -----------------------------------------------------         
            String last_amount_value = dao.getValueByCol("bi_monitor_order_info_rslt", rowkey_amount_last,
                    "order_amount");
            String last_amount_col_data_value = dao.getValue("bi_monitor_order_info_rslt",
                        rowkey_amount_col_data);
// ----------------------------------------------------
            if (!"".equals(last_amount_value)) {
                orderAmountTotal = Double.parseDouble(last_amount_value);
            }
            if (!"".equals(last_amount_col_data_value)) {
                sFinal = last_amount_col_data_value;
            }

            /**
             * 订单数
             */
            rowkey_ordr_num_last = "last_ordr_num_" + curTime;
            rowkey_ordr_num_col_data = "col_data_ordr_num_" + curTime;
            /** 读取Hbase获取 */
            String last_ordr_num_value = dao.getValueByCol("bi_monitor_order_info_rslt", rowkey_ordr_num_last,
                                                           "order_num");
            String last_ordr_num_col_data_value = dao.getValue("bi_monitor_order_info_rslt",
                                                               rowkey_ordr_num_col_data);
            if (!"".equals(last_ordr_num_value)) {
                orderNumTotal = Long.parseLong(last_ordr_num_value);
            }
            if (!"".equals(last_ordr_num_col_data_value)) {
                sFinalOrdrNum = last_ordr_num_col_data_value;
            }

            /**
             * 这里补充:如果在sFinal中找不到当前小时的字符串,则进行appendNextHourStr的操作。由于重启过程中,跨小时了,这个时候curHour ==
             * orderCreateHour,进行replaceNowHourStr操作,不会在当前小时的柱子上进行添加,所以需要补充一个小时的柱子
             */
            if (sFinal.indexOf("[" + curHour + ",") == -1) {
                sFinal = CommonUtil.appendNextHourStr(curHour, sFinal, "0.0");
                sFinalOrdrNum = CommonUtil.appendNextHourStr(curHour, sFinalOrdrNum, "0");
            }

            logger.info("sFinal : " + sFinal);
            logger.info("sFinalOrdrNum :" + sFinalOrdrNum);
            /** 获取柱状图最后一个小时的数值用来作为初始值 */
            orderAmountHourTotal = Double.parseDouble(sFinal.substring(sFinal.lastIndexOf(",") + 1, sFinal.length() - 2));
            orderNumHourTotal = Long.parseLong(sFinalOrdrNum.substring(sFinalOrdrNum.lastIndexOf(",") + 1,
                                                                       sFinalOrdrNum.length() - 2));

            logger.info("===curHour===" + curHour);
            logger.info("amount init value is:   " + orderAmountTotal);
            logger.info("col data init value is:   " + sFinal);
            logger.info("ordr num init value is:   " + orderNumTotal);
            logger.info("col data ordr num init value is:   " + sFinalOrdrNum);

            dBConnection = new OracleConnection();
            con = dBConnection.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void insertOracle(Connection con, Double orderAmountTotal, long orderNumTotal) {
        Statement statement = null;
        try {
            statement = con.createStatement();
            String date_time_id = DateUtil.getFmtDate(null, DateUtil.YYYY_MM_DD_HH_MM_STR);
            String sql = "insert into rpt.rpt_realtime_mp_sale(date_time_id,ordr_num,pm_amt)" + " values (to_date('"
                         + date_time_id + "','yyyy-MM-dd hh24:mi'),'" + orderNumTotal + "','" + orderAmountTotal + "')";
            //LOG.info(sql);
            try {
                statement.execute(sql);
            } catch (Exception e) {
            	logger.error("error: OrderInfoBolt rpt.rpt_realtime_mp_sale error ! ", e);
            }
        } catch (Exception e) {
        	logger.error("error: OrderInfoBolt rpt.rpt_realtime_mp_sale error ! ", e);
            // e.printStackTrace() ;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

	@Override
	public Object handleOrder(Tuple tuple, JumpMQOrderVo parentOrder, JumpMQOrderVo order) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double handleSoItem(Tuple tuple, JumpMQOrderVo parentOrder, JumpMQOrderVo order, JumpMQOrderItemVo soItem) {
		// 业务逻辑，如果金额和积分同时为零，则不算成交
		if(!BusinessLogic.isTransation(soItem)){
			return 0D;
		}
		
		// 积分兑换
		if (soItem.getOrderItemAmount().compareTo(new BigDecimal("0")) == 0) {
			return 0D;
		}
		
		// 成交净额口径
		double pmNetAmt = soItem.getOrderItemAmount().doubleValue()
				- soItem.getPromotionAmount().doubleValue()
				- soItem.getCouponAmount().doubleValue() 
				- soItem.getOrderItemAmount().doubleValue()
				/ order.getOrderAmount().doubleValue()
				* order.getOrderPaidByRebate().doubleValue();
		
		logger.info("pmNetAmt:" + pmNetAmt + soItem.baseInfo());
		return pmNetAmt;
	}
	
	@Override
	public Double processSoItem(Tuple input, JumpMQOrderVo order) {	
		double orderAmt = 0.0D;
		
		logger.info("Message successfully received: " + order.baseInfo());
		
		if (order.getChildOrderList()!=null && !order.getChildOrderList().isEmpty()) {
			for(JumpMQOrderVo childOrder : order.getChildOrderList())
			{
				logger.info("Order successfully received: " + childOrder.baseInfo());
				
				List<JumpMQOrderItemVo> soItemList = childOrder.getSoItemList() ;
				
				for(JumpMQOrderItemVo soItem : soItemList)
				{
					if (soItem.getSubSoItemList() != null && !soItem.getSubSoItemList().isEmpty()) {
						
						List<JumpMQOrderItemVo> subSoItemList = soItem.getSubSoItemList();
						
						for (JumpMQOrderItemVo subSoItem : subSoItemList) {
							
							logger.info("SoItem successfully received: " + subSoItem.baseInfo());
							orderAmt += handleSoItem(input, order, childOrder, subSoItem);
							logger.info("SoItem successfully processed: " + subSoItem.baseInfo());
						}
					}
					else {
						logger.info("SoItem successfully received: " + soItem.baseInfo());
						orderAmt += handleSoItem(input, order, childOrder, soItem);
						logger.info("SoItem successfully processed: " + soItem.baseInfo());
					}
				}
				logger.info("Order successfully processed: " + childOrder.baseInfo());
			}
		}else {
			logger.info("Order successfully received: " + order.baseInfo());
			
			List<JumpMQOrderItemVo> soItemList = order.getSoItemList();
			
			for(JumpMQOrderItemVo soItem : soItemList)
			{
				if (soItem.getSubSoItemList() != null && !soItem.getSubSoItemList().isEmpty()) {
					
					List<JumpMQOrderItemVo> subSoItemList = soItem.getSubSoItemList();
					
					for (JumpMQOrderItemVo subSoItem : subSoItemList) {
						
						logger.info("SoItem successfully received: " + subSoItem.baseInfo());
						orderAmt += handleSoItem(input, null, order, subSoItem);
						logger.info("SoItem successfully processed: " + subSoItem.baseInfo());
		
					}
					
				}
				else {
					logger.info("SoItem successfully received: " + soItem.baseInfo());
					orderAmt += handleSoItem(input, null, order, soItem);
					logger.info("SoItem successfully processed: " + soItem.baseInfo());
				}
			}
			logger.info("Order successfully processed: " + order.baseInfo());
		}
		
		return orderAmt;
	}
	
	public static void main(String args[]) {
		
	}
	
}
