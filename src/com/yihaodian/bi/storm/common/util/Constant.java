package com.yihaodian.bi.storm.common.util;

public class Constant {
	
	public static final String GLOBAL_CONFIG_PATH = "/var/www/webapps/config";
	/** 10分钟 */
	public static final int MINUTE_10 = 10 * 60 * 1000;
	/** 1分钟 */
	public static final int MINUTE_1 = 60 * 1000;
	/** 50s */
	public static final int SECOND_50 = 50 * 1000;
	/** 30秒 */
	public static final int SECOND_30 = 30 * 1000;
	/** 5秒 */
	public static final int SECOND_5 = 5 * 1000;

	/** kafka */
	// public final static String zkConnect =
	// "192.168.112.138:2181,192.168.112.139:2181,192.168.112.140:2181"; // 开发环境
	public final static String KAFKA_ZK_CONNECTION = "10.4.11.85:2181,10.4.11.86:2181,10.4.11.87:2181,10.4.11.88:2181,10.4.11.89:2181"; // 生产环境
	public final static String KAFKA_GROUP_ID = "groupBI"; // 默认kafka
	// ComsumerID
	// ,该值会调用时传人参数替换
	public final static String KAFKA_CONSUMER_NAME_TRACKER = "tracker";
	public final static String KAFKA_CONSUMER_NAME_UV = "c_uv_comsumer";

	public final static String SPOUT_ID_ORDER_SOURCE = "OrderSourceSpout";
	
	// uv分平台 app h5
	public final static String KAFKA_CONSUMER_NAME_UV_WITH_PLATFORM = "c_uv_with_platform_comsumer";

	public final static String MQ_BASE_CUSTOMER_NAME = "bi";
	/** spout的CONSUMERNAME，每个必须不同 */
	public final static String MQ_BI_ORDER_CUSTOMER = MQ_BASE_CUSTOMER_NAME
			+ "_order_customer";
	/** spout的CONSUMERNAME，每个必须不同 */
	public final static String MQ_BI_ORDER_INFO = MQ_BASE_CUSTOMER_NAME
			+ "_order_info";
	/** spout的CONSUMERNAME，每个必须不同 */
	public final static String MQ_BI_BUS_ORDER_INFO = MQ_BASE_CUSTOMER_NAME
			+ "_bus_order_info";

	public final static String MQ_BI_MOBILE_ORDER_INFO = MQ_BASE_CUSTOMER_NAME
			+ "_mobile_order_info";

	/** spout的CONSUMERNAME，每个必须不同 */
	public final static String MQ_BI_CTG_JMQ_CONSUMERNAME = MQ_BASE_CUSTOMER_NAME
			+ "_ctg_jmq_consumerName";

	/** spout的CONSUMERNAME，每个必须不同 */
	public final static String MQ_BI_META_DATA_ORDER = MQ_BASE_CUSTOMER_NAME
			+ "_meta_data_order";
	
	/** spout的CONSUMERNAME，每个必须不同 */
	public final static String MQ_BI_RPT_REALTIME_MONTR_TRANX= MQ_BASE_CUSTOMER_NAME
			+ "_rpt_realtime_montr_tranx_s";
	
	/** spout的CONSUMERNAME，每个必须不同 */
	public final static String MQ_BI_RPT_REALTIME_MONTR_CANCL= MQ_BASE_CUSTOMER_NAME
			+ "_rpt_realtime_montr_cancl_s";
	

	
	
	// PV 层级
	public final static Integer PV_EXECUTER_NUM = 5;
	
	// 支付类型
    public final static Integer ONLINE_PAY = 1;
    public final static Integer OFFLINE_PAY = 2;
 
	
	
	//-------------------------------------表名--------------------------------------------
	
	public final static String COMMON_FAMILY = "cf";//family 名称统一 统计函数需要制定family
	
	/**字典表，用于记录一下表操作最后时间等*/
	public final static String TABLE_DICT="bi_monitor_dictionary";
	public final static String COLUMN_DICT_LASTTIME="lastOptTime";
	public final static String ROWKEY_DICT_CUSTOMER_RSLT="customer_rslt";//顾客数结果表最后操作时间在字典表里的rowkey
	public final static String ROWKEY_DICT_CUSTOMER_TG_RSLT="customer_tg_rslt";
	public final static String ROWKEY_DICT_CUSTOMER_SG_RSLT="customer_sg_rslt";
	public final static String ROWKEY_DICT_MOBILE_CUSTOMER_RSLT="mobile_customer_rslt";
	
	public final static String BI_MONITOR_PRODUCT_CATEG="bi_monitor_product_categ";//商品类目映射表

	/** 4.2.2 顾客数  */
	public final static String TABLE_CUSTOMER_RESULT = "bi_monitor_customer_rslt";
	public final static String TABLE_CUSTOMER_TMP = "bi_monitor_customer_tmp";
	public final static String TOPIC_CUSTOMER_RESULT = "bi_monitor_customer_topic";
	public final static String TABLE_CUSTOMER_YHDSELF_RESULT = "bi_monitor_customer_yhd_rslt"; // 自营
	public final static String TABLE_CUSTOMER_NON_YHDSELF_RESULT = "bi_monitor_customer_nonyhd_rslt";  // 非自营
	
	/** 4.2.4 移动顾客数  */
	public final static String TABLE_MOBILE_CUSTOMER_RESULT = "bi_monitor_mobile_customer_rslt";
	public final static String TABLE_MOBILE_CUSTOMER_TMP = "bi_monitor_mobile_customer_tmp";
	
	
	
	/** 4.2.1 uv */
	public final static String TABLE_UV_RESULT = "bi_monitor_uv_rslt";//记录历史点
	public final static String TABLE_UV_TMP = "bi_monitor_uv_tmp";//去重count取uv
	public final static String UV_PRODUCER_NAME = "total_uv";//rocketmq消息主题
	
	/** 4.4.3 4.4.4 业务模式 顾客数*/
	public final static String TABLE_CUSTOMER_BY_TYPE_RESULT = "bi_monitor_customer_by_type_rslt";
	public final static String TABLE_CUSTOMER_BY_TYPE_TMP = "bi_monitor_customer_by_type_tmp";
	/** 业务模式 */
	public final static String SG_TYPE = "sg";//闪购
	public final static String TG_TYPE = "tg";//团购
	

	/**4.2.4地区顾客数 4.3.2地区产品数 4.3.2地区产品净额**/
	public final static String TABLE_AREA_ACCUMULATIVE_RSLT = "bi_monitor_area_accumulative_rslt";
	public final static String TABLE_AREA_CUSTOMER_TMP = "bi_monitor_area_customer_tmp";
	public final static String TABLE_AREA_PRO_TMP = "bi_monitor_area_pro_tmp";
	public final static String RMQ_TOPIC_PRODUCTOR_NAME = "bi_rmq_area_amt_productor";
	public final static String RMQ_TOPIC_CONSUMER_NAME = "bi_rmq_area_amt_consumer";
	public final static String RMQ_TOPIC_AREA_AMT = "bi_rmq_topic_area_amt";
	
	/** 北上广深 订单净额 by zhangsheng2 20141111 */
	public final static String TABLE_CORE_CITY_ORDERINFO_RSLT = "bi_core_city_orderinfo";

	

	/** 4.3.5 成交订单净额 成交订单订单数*/
	public final static String TABLE_ORDER_INFO_RESULT = "bi_monitor_order_info_rslt"; //  2015-06-15 15:28 改 用于测试
	
	public final static String TABLE_BUS_ORDER_INFO_RESULT = "bi_monitor_bus_order_info_rslt";
	
	/** 4.2.4 移动成交订单净额 移动成交订单订单数*/
	public final static String TABLE_MOBILE_ORDER_INFO_RESULT = "bi_monitor_mobile_order_rslt";
	
	
	/** 4.1.1 当前在线人数     按省份  */
	public final static String TABLE_ONLINE_PRO_UV_TMP = "bi_monitor_online_prvc_uv_tmp";
	public final static String TABLE_ONLINE_PRO_UV_RSLT = "bi_monitor_online_prvc_uv_rslt";
	
	/** 一级品类相关   */
	public final static String TABLE_CTG_RSLT = "bi_monitor_ctg_rslt";
	public final static String TABLE_CTG_RSLT_NEW = "bi_monitor_categ_rslt";
	public final static String TABLE_CTG_SKU_TMP = "bi_monitor_ctg_sku";
	public final static String TABLE_CTG_CUSTOMER_TMP = "bi_monitor_ctg_customer";

	
	
	/** tracker uv */
	public final static String TABLE_TRACKER_UV_RESULT = "bi_tracker_uv_rslt";
	/** tracker pv */
	public final static String TABLE_TRACKER_PV_RESULT = "bi_tracker_pv_rslt";

	
	/** 团闪相关表 */
	public static final String TABLE_GUID_UID = "bi_guid_uid_dim";
	public static final String TABLE_GROUPON_USER = "bi_groupon_user";
	public static final String TABLE_MP_USER = "bi_mp_user";
	public static final String TABLE_DIM_GROUPON = "dim_grpon";
	public static final String TABLE_GROUPON_MP_AMT = "bi_grpon_mp_amt";
	public static final String TABLE_TRACK_GROUPON_MP_DETL = "rpt_realtime_gm_track_detl";
	public static final String TABLE_TRACK_SESSION_UID_DETL = "rpt_realtime_session_uid_detl";
	public static final String TABLE_ORDER_GROUPON_MP_DETL = "rpt_realtime_gm_ordr_detl";
	//---------------------------------------渠道标志-------------------------------------------
	public static final String SEO = "5"; // 搜索引擎SEO
    public static final String DIRECT = "9"; // 直接访问
    public static final String UNKNOWN = "12"; // 未知流量
	
	//--------------------------------------- HBase 表 ----------------------------------------
    public static final String HBASE_RPT_CATEG_UV = "bi_rpt_categ_uv";
	public static final String HBASE_RPT_AMT = "rpt_realtime_mp_amt";
	public static final String HBASE_DIM_CHANNEL = "bi_dim_chanl";
	
	
	//测试表
	public static final String TABLE_PV_COUNT_TEST="pv_count_test" ;
	public static final String TABLE_TRACKER_PV_TEST = "bi_tracker_pv_rslt_tst";
	//--------------------------------------- Oracle 表 ---------------------------------------
	// 测试表
	public static final String TABLE_RPT_ORDR_TEST     = "hujun1.rpt_realtime_mp_ordr";
	public static final String TABLE_RPT_ORDR_CTG_TEST = "hujun1.rpt_realtime_mp_categ_ordr";
	public static final String TABLE_RPT_CUST_TEST     = "hujun1.rpt_realtime_mp_user";
	public static final String TABLE_RPT_CUST_CTG_TEST = "hujun1.rpt_realtime_mp_categ_user";
	public static final String TABLE_RPT_SALE_TEST     = "hujun1.rpt_realtime_mp_amt";
	public static final String TABLE_RPT_SALE_CTG_TEST = "hujun1.rpt_realtime_mp_categ_amt";
	public static final String TABLE_RPT_REALTIME_MONTR_CANCL_S_TEST = "lining2.rpt_realtime_montr_cancl_s";
	public static final String TABLE_RPT_REALTEIM_MONTR_TRANX_S_TEST = "lining2.rpt_realtime_montr_tranx_s";
	public static final String TABLE_FCT_TRANS_DETL = "hujun1.fct_trans_detl";
	public static final String TABLE_FCT_CANCEL_DETL = "hujun1.fct_cancel_detl";
	

	
	// 正式表
	
	public static final String TABLE_RPT_REALTIME_MONTR_CANCL_S = "rpt.rpt_realtime_montr_cancl_s";
	public static final String TABLE_RPT_REALTEIM_MONTR_TRANX_S = "rpt.rpt_realtime_montr_tranx_s";
	public static final String TABLE_RPT_ORDR     = "rpt.rpt_realtime_mp_ordr_s";
	public static final String TABLE_RPT_ORDR_CTG = "rpt.rpt_realtime_mp_categ_ordr_s";
	public static final String TABLE_RPT_CUST     = "rpt.rpt_realtime_mp_user_s";
	public static final String TABLE_RPT_CUST_CTG = "rpt.rpt_realtime_mp_categ_user_s";
	public static final String TABLE_RPT_SALE     = "rpt.rpt_realtime_mp_amt_s";
	public static final String TABLE_RPT_SALE_CTG = "rpt.rpt_realtime_mp_categ_amt_s";

	//---------------------------------------Component ID--------------------------------------
	// Spout
	public static final String SPOUT_TRANSACTION = "TransactionSpout";
	public static final String SPOUT_TRANSDETL="TransDetlSpout";

	public static final String SQOUT_RPTMONTRCANCL="RptMontrCancelSpout";
	public static final String SQOUT_RPTREALTIMEMONTRTRANX="RptRealtimeMontrTranxSpout";
	
	public static final String SPOUT_ORDERCANCEL = "OrderCancelSpout";
	public static final String SPOUT_ORDER_CREATE = "OrderCreateSpout";
	public static final String SPOUT_PAYMENT_UPDATE = "PaymentUpdateSpout";
	public static final String SPOUT_SALES_AMT_CANCL = "SalesAmtCanclSpout";
	public static final String SPOUT_ONLINE_TRANS = "OnlineTrans";
	public static final String SPOUT_OFFLINE_TRANS = "OfflineTrans";
	
	// Bolt
	public static final String BOLT_CUSTOMER = "CustomerBolt";
	public static final String BOLT_ORDER = "OrderBolt";
	public static final String BOLT_SALE = "SaleBolt";
	public static final String BOLT_TRANS_DETL = "TransDetl";
	public static final String BOLT_CANCEL_DETL = "CancelDetl";
	
	public static final String BOLT_RPT_REALTIME_MONTR_TRANX = "RptRealtimeMontrTranxBolt";
	public static final String BOLT_RPT_REALTIME_MONTR_CANCL = "RptRealtimeMontrCanclBolt";
	
	public static final String BOLT_SALES_AMT_CANCL = "RealtimeSalesAmtCanclBolt";
	
	//----------------------------------------消费ID-------------------------------------------
	//----格式： CID_APPNAME = "bi_appname"---
	public static final String CID_MOBILEREPORT = "bi_mobilereport";
	public static final String CID_SALESAMTCANCL = "bi_SalesAmtCancl";
	public final static String CID_PV = "bi_pv";

}
