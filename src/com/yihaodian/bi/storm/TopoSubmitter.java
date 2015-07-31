package com.yihaodian.bi.storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift7.TException;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TopologySummary;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.yihaodian.bi.common.Debug;
import com.yihaodian.bi.storm.business.mobile.so.MobileCustomerBolt;
import com.yihaodian.bi.storm.business.mobile.so.MobileOrderBolt;
import com.yihaodian.bi.storm.business.mobile.so.MobileSaleBolt;
import com.yihaodian.bi.storm.business.mobile.so.RptRealtimeMontrCanclBolt;
import com.yihaodian.bi.storm.business.mobile.so.RptRealtimeMontrTranxBolt;
import com.yihaodian.bi.storm.business.so.catgNew.CtgPutHBaseBolt;
import com.yihaodian.bi.storm.business.so.catgNew.CtgSoRsltBolt;
import com.yihaodian.bi.storm.business.so.customer.CustomerBolt;
import com.yihaodian.bi.storm.business.so.detl.CancelDetlBolt;
import com.yihaodian.bi.storm.business.so.detl.TransDetlBolt;
import com.yihaodian.bi.storm.business.so.orderinfo.BusinessOrderInfoBolt;
import com.yihaodian.bi.storm.business.so.orderinfo.CoreCityAmtBolt;
import com.yihaodian.bi.storm.business.so.orderinfo.MetaDataOrderBolt;
import com.yihaodian.bi.storm.business.so.orderinfo.MobileOrderInfoBolt;
import com.yihaodian.bi.storm.business.so.orderinfo.OrderInfoBolt;
import com.yihaodian.bi.storm.business.so.orderinfo.SalesAmtCanclBolt;
import com.yihaodian.bi.storm.business.so.orderinfo.SalesBolt;
import com.yihaodian.bi.storm.business.so.promotion.GrouponMpOrderFilterBolt;
import com.yihaodian.bi.storm.business.so.promotion.GrouponOrderBolt;
import com.yihaodian.bi.storm.business.so.promotion.GrponMpOrdrDetlBolt;
import com.yihaodian.bi.storm.business.so.promotion.MpOrderBolt;
import com.yihaodian.bi.storm.business.tracker.TrackerRecountBolt;
import com.yihaodian.bi.storm.business.tracker.dim.TrackGrouponUidBolt;
import com.yihaodian.bi.storm.business.tracker.dim.TrackGuidUidBolt;
import com.yihaodian.bi.storm.business.tracker.dim.TrackMpUidBolt;
import com.yihaodian.bi.storm.business.tracker.groupon.TrackGrponDetlBolt;
import com.yihaodian.bi.storm.business.tracker.groupon.TrackMpDetlBolt;
import com.yihaodian.bi.storm.business.tracker.groupon.TrackerFilterBolt;
import com.yihaodian.bi.storm.business.tracker.pv.PVBolt;
import com.yihaodian.bi.storm.business.tracker.pv.PVIncBolt;
import com.yihaodian.bi.storm.business.tracker.pv.PvCountBolt;
import com.yihaodian.bi.storm.business.tracker.pv.TrackerPvShuffleBolt;
import com.yihaodian.bi.storm.business.tracker.session.TrackSidGuidUidBolt;
import com.yihaodian.bi.storm.business.tracker.uv.TrackerUvFieldsBolt;
import com.yihaodian.bi.storm.business.tracker.uv.UvCountBolt;
import com.yihaodian.bi.storm.business.tracker.uv.UvWithPlatformAppCountBolt;
import com.yihaodian.bi.storm.business.tracker.uv.UvWithPlatformFieldsBolt;
import com.yihaodian.bi.storm.business.tracker.uv.UvWithPlatformH5CountBolt;
import com.yihaodian.bi.storm.business.tracker.uv.UvWithPlatformRecountBolt;
import com.yihaodian.bi.storm.common.spout.OfflineTransSpout;
import com.yihaodian.bi.storm.common.spout.OnlineTransSpout;
import com.yihaodian.bi.storm.common.spout.OrderCancelSpout;
import com.yihaodian.bi.storm.common.spout.OrderSourceSpout;
import com.yihaodian.bi.storm.common.spout.TrackBatchSpout;
import com.yihaodian.bi.storm.common.spout.TransDetlSpout;
import com.yihaodian.bi.storm.common.spout.TransactionSpout;
import com.yihaodian.bi.storm.common.util.Constant;
import com.yihaodian.bi.storm.logic.tracker.bolt.UVShuffleBolt;
import com.yihaodian.bi.storm.logic.tracker.bolt.UvCacheBolt;
import com.yihaodian.bi.storm.logic.tracker.spout.KafkaTrackerSpout;

/**
 * 类TopoSubmiter.java的实现描述：TODO 类实现描述
 * 
 * @author zhaoheng Dec 27, 2014 6:41:18 PM
 */
public class TopoSubmitter {

    /**
     * Key: TopoID <BR>
     * TopoGroup: include Spout and Bolt
     */
    private static final Map<String, TopoGroup> tops              = new HashMap<String, TopoGroup>();
    private static final long                   RESTART_WAIT_TIME = 5000;

    public static void putTopo(TopoGroup tg) {
        TopoGroup oldTg = tops.get(tg.getName());
        if (oldTg != null) {
            throw new RuntimeException("TopoGroup [" + tg.getName() + "] exist");
        }
        tops.put(tg.getName(), tg);
    }

    public static boolean confirm(String msg) {
        System.out.print(msg + " (y/n)?");
        BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
        try {
            String line = r.readLine();
            if (line != null) {
                String info = line.trim().toLowerCase();
                if (info.equals("y") || info.equals("yes")) {
                    return true;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                r.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    static {
        // 商城，自营销售净额
        putTopo(new TopoGroup("BusinessModeSales") {

            @Override
            public void init(String[] args) {
                builder().setSpout("OrderSourceSpout", new OrderSourceSpout(Constant.MQ_BI_BUS_ORDER_INFO + "_new"), 1);
                builder().setBolt("BusinessOrderInfoBolt", new BusinessOrderInfoBolt(), 1).shuffleGrouping("OrderSourceSpout");

                conf().setDebug(false);
                conf().setNumWorkers(3);
            }

            @Override
            public String help() {
                return "商城，自营销售净额";
            }
        });

        // 全站分品类销售净额、订单数，以及分品类顾客明细和sku明细
        putTopo(new TopoGroup("CategSales") {

            @Override
            public void init(String[] args) {
                builder().setSpout(Constant.SPOUT_ID_ORDER_SOURCE,
                                   new OrderSourceSpout(Constant.MQ_BI_CTG_JMQ_CONSUMERNAME + "_tst"), 1);
                builder().setBolt("CtgSoRslt", new CtgSoRsltBolt(), 1).shuffleGrouping("OrderSourceSpout");
                builder().setBolt("CtgPutHBase", new CtgPutHBaseBolt(), 3).shuffleGrouping("OrderSourceSpout");
                conf().setDebug(false);
                conf().setNumWorkers(10);
            }

            @Override
            public String help() {
                return "全站分品类销售净额、订单数，以及分品类顾客明细和sku明细";
            }
        });
        // 全站分钟顾客数,和每日销售净额、订单数
        putTopo(new TopoGroup("CustomersAndSales") {

            @Override
            public void init(String[] args) {
                builder().setSpout("OrderSourceSpout", new OrderSourceSpout(Constant.MQ_BI_ORDER_INFO + "_new"), 1);
                builder().setBolt("CustomerBolt", new CustomerBolt(), 1).shuffleGrouping("OrderSourceSpout");
                builder().setBolt("OrderInfoBolt", new OrderInfoBolt(), 1).shuffleGrouping("OrderSourceSpout");
                conf().setDebug(false);
                conf().setNumWorkers(3);
            }

            @Override
            public String help() {
                return "全站分钟顾客数,和每日销售净额、订单数";
            }
        });

        // 全站及品类销售和顾客明细（手机端推送）
        putTopo(new TopoGroup("MobileReport") {

            @Override
            public void init(String[] args) {
                builder().setSpout(Constant.SPOUT_OFFLINE_TRANS, new OfflineTransSpout(Constant.CID_MOBILEREPORT), 1);
                builder().setSpout(Constant.SPOUT_ONLINE_TRANS, new OnlineTransSpout(Constant.CID_MOBILEREPORT), 1);
                // builder().setSpout(Constant.SPOUT_ORDERCANCEL, new OrderCancelSpout(Constant.CID_MOBILEREPORT), 1);

                builder().setBolt(Constant.BOLT_CUSTOMER, new MobileCustomerBolt(), 1).shuffleGrouping(Constant.SPOUT_OFFLINE_TRANS).shuffleGrouping(Constant.SPOUT_ONLINE_TRANS);

                builder().setBolt(Constant.BOLT_ORDER, new MobileOrderBolt(), 1).shuffleGrouping(Constant.SPOUT_OFFLINE_TRANS).shuffleGrouping(Constant.SPOUT_ONLINE_TRANS);

                builder().setBolt(Constant.BOLT_SALE, new MobileSaleBolt(), 1).shuffleGrouping(Constant.SPOUT_OFFLINE_TRANS).shuffleGrouping(Constant.SPOUT_ONLINE_TRANS);
                // .shuffleGrouping(Constant.SPOUT_ORDERCANCEL);

                conf().setDebug(false);
                conf().setNumWorkers(6);
            }

            @Override
            public String help() {
                return "全站及品类销售和顾客明细（手机端推送）";
            }
        });

        // 全站订单明细信息入库Oracle
        putTopo(new TopoGroup("Orderinfo2db") {

            @Override
            public void init(String[] args) {
                builder().setSpout("OrderSourceSpout", new OrderSourceSpout(Constant.MQ_BI_META_DATA_ORDER), 1);
                builder().setBolt("MetaDataOrderBolt", new MetaDataOrderBolt(), 1).shuffleGrouping("OrderSourceSpout");

                conf().setDebug(false);
                conf().setNumWorkers(3);
            }

            @Override
            public String help() {
                return "全站订单明细信息入库Oracle";
            }
        });
        // 移动成交订单净额
        putTopo(new TopoGroup("MobileSales") {

            @Override
            public void init(String[] args) {
                builder().setSpout("OrderSourceSpout", new OrderSourceSpout(Constant.MQ_BI_MOBILE_ORDER_INFO + "_new"),
                                   1);
                builder().setBolt("MobileOrderInfoBolt", new MobileOrderInfoBolt(), 1).shuffleGrouping("OrderSourceSpout");

                conf().setDebug(false);
                conf().setNumWorkers(3);
            }

            @Override
            public String help() {
                return "移动成交订单净额";
            }
        });
        // 城市成交订单净额
        putTopo(new TopoGroup("SalesByCity") {

            @Override
            public void init(String[] args) {
                builder().setSpout("sp", new OrderSourceSpout("bi_core_city_amt_new"), 1);
                builder().setBolt("coreCityAmt", new CoreCityAmtBolt(), 1).shuffleGrouping("sp");
                conf().setDebug(false);
                conf().setNumWorkers(3);
            }

            @Override
            public String help() {
                return "城市成交订单净额";
            }
        });
        // 团闪小口径销售净额
        putTopo(new TopoGroup("GrponMpOrder") {

            @Override
            public void init(String[] args) {
                builder().setSpout("orderspout", new OrderSourceSpout("bi_grpon_mp_order_amt"), 1);
                builder().setBolt("filter", new GrouponMpOrderFilterBolt(), 5).shuffleGrouping("orderspout");
                builder().setBolt("grpon", new GrouponOrderBolt()).shuffleGrouping("filter", "grpon");
                builder().setBolt("mp", new MpOrderBolt()).shuffleGrouping("filter", "mp");
                conf().setDebug(true);
                conf().setNumWorkers(6);
            }

            @Override
            public String help() {
                return "团闪小口径销售净额";
            }
        });
        // 全站用户(guid,uid)维表
        putTopo(new TopoGroup("TrackGuidUid") {

            @Override
            public void init(String[] args) {
                builder().setSpout("trackspout", new TrackBatchSpout("bi_tracker_groupon_mp_new"), 3);
                builder().setBolt("guid", new TrackGuidUidBolt(), 5).shuffleGrouping("trackspout");
                builder().setBolt("filter", new TrackerFilterBolt(), 5).shuffleGrouping("trackspout");
                builder().setBolt("grpon", new TrackGrouponUidBolt(), 3).shuffleGrouping("filter", "grpon_pc").shuffleGrouping("filter",
                                                                                                                               "grpon_h5").shuffleGrouping("filter",
                                                                                                                                                           "grpon_app");
                builder().setBolt("mp", new TrackMpUidBolt(), 3).shuffleGrouping("filter", "mp_pc").shuffleGrouping("filter",
                                                                                                                    "mp_h5").shuffleGrouping("filter",
                                                                                                                                             "mp_app");
                conf().setDebug(false);
                conf().setNumWorkers(10);
            }

            @Override
            public String help() {
                return "全站用户(guid,uid)维表";
            }
        });
        // 团闪订单明细(数据入Oracle)
        putTopo(new TopoGroup("GrponMpOrdrDetl") {

            @Override
            public void init(String[] args) {
                builder().setSpout(Constant.SPOUT_TRANSACTION, new TransactionSpout("bi_grpon_mp_order_detl_new"), 1);
                builder().setBolt("filter", new GrouponMpOrderFilterBolt(), 3).shuffleGrouping(Constant.SPOUT_TRANSACTION);
                builder().setBolt("grpon_mp", new GrponMpOrdrDetlBolt(), 3).shuffleGrouping("filter", "grpon").shuffleGrouping("filter",
                                                                                                                               "mp");
                conf().setDebug(false);
                conf().setNumWorkers(7);
            }

            @Override
            public String help() {
                return "团闪订单明细(数据入Oracle)";
            }
        });
        // 团闪小口径流量明细(数据入Oracle)
        putTopo(new TopoGroup("GrponMpTrackDetl") {

            @Override
            public void init(String[] args) {
                builder().setSpout("trackspout", new TrackBatchSpout("bi_tracker_grpon_mp_detl_new"), 1);
                builder().setBolt("filter", new TrackerFilterBolt(), 5).shuffleGrouping("trackspout");
                builder().setBolt("grpon", new TrackGrponDetlBolt(), 3).shuffleGrouping("filter", "grpon_pc").shuffleGrouping("filter",
                                                                                                                              "grpon_h5").shuffleGrouping("filter",
                                                                                                                                                          "grpon_app");
                builder().setBolt("mp", new TrackMpDetlBolt(), 3).shuffleGrouping("filter", "mp_pc").shuffleGrouping("filter",
                                                                                                                     "mp_h5").shuffleGrouping("filter",
                                                                                                                                              "mp_app");
                conf().setDebug(false);
                conf().setNumWorkers(12);
            }

            @Override
            public String help() {
                return "团闪小口径流量明细(数据入Oracle)";
            }
        });
        // 全站会话id, guid，用户id对应关系明细(数据入Oracle)
        putTopo(new TopoGroup("SidGuidUid") {

            @Override
            public void init(String[] args) {
                builder().setSpout("trackspout", new TrackBatchSpout("bi_tracker_sid_guid_uid_new"), 5);
                builder().setBolt("sid_guid_uid", new TrackSidGuidUidBolt(), 5).shuffleGrouping("trackspout");
                conf().setDebug(false);
                conf().setNumWorkers(10);
            }

            @Override
            public String help() {
                return "全站会话id, guid，用户id对应关系明细(数据入Oracle)";
            }
        });
        // 全站PV，UV统计
        putTopo(new TopoGroup("PVUV") {

            @Override
            public void init(String[] args) {
                builder().setSpout("spout", new TrackBatchSpout(Constant.KAFKA_CONSUMER_NAME_UV + "_new"), 3);
                // 处理tracker list 每个list <=100条数据 开10个并发 处理量为10个list 1000条数据
                builder().setBolt("recount", new TrackerRecountBolt(), 10).shuffleGrouping("spout");
                // 分组处理guid 判断发送消息
                builder().setBolt("fielduv", new TrackerUvFieldsBolt(), 12).fieldsGrouping("recount",
                                                                                           new Fields("guid"));
                // 计算uv 入库
                builder().setBolt("dealuv", new UvCountBolt(), 1).shuffleGrouping("fielduv");
                // 计算pv 入库
                builder().setBolt("dealpv", new PVBolt(), 1).shuffleGrouping("spout");

                conf().setDebug(false);
                conf().setNumWorkers(27);
            }

            @Override
            public String help() {
                return "全站PV，UV统计";
            }
        });
        // 全站PV，UV统计
        putTopo(new TopoGroup("NEW_PVUV") {

            @Override
            public void init(String[] args) {
                builder().setSpout("spout", new TrackBatchSpout(Constant.KAFKA_CONSUMER_NAME_UV + "_new"), 3);
                // 处理tracker list 每个list <=100条数据 开10个并发 处理量为10个list 1000条数据
                builder().setBolt("recount", new TrackerRecountBolt(), 10).shuffleGrouping("spout");
                // 分组处理guid 判断发送消息
                builder().setBolt("fielduv", new TrackerUvFieldsBolt(), 12).fieldsGrouping("recount",
                                                                                           new Fields("guid"));
                // 计算uv 入库
                builder().setBolt("dealuv", new UvCountBolt(), 1).shuffleGrouping("fielduv");
                // 并发累加pv 传入下一级bolt,传过去的值*并发数就是全局pv数
                builder().setBolt("shufflepv", new TrackerPvShuffleBolt(), Constant.PV_EXECUTER_NUM).shuffleGrouping("recount");
                // 计算pv 入库
                builder().setBolt("dealpv", new PvCountBolt(), 1).shuffleGrouping("shufflepv");

                conf().setDebug(false);
                conf().setNumWorkers(10);
            }

            @Override
            public String help() {
                return "全站PV，UV统计";
            }
        });
        // 分平台（app，h5）统计UV
        putTopo(new TopoGroup("PVUVbyPlatform") {

            @Override
            public void init(String[] args) {
                builder().setSpout("uvWithPlatformSpout",
                                   new TrackBatchSpout(Constant.KAFKA_CONSUMER_NAME_UV_WITH_PLATFORM + "_new"), 3);
                // 处理tracker list 每个list <=100条数据 开10个并发 处理量为10个list 1000条数据
                builder().setBolt("recountUvWithPlatform", new UvWithPlatformRecountBolt(), 5).shuffleGrouping("uvWithPlatformSpout");
                // 分组处理guid 判断发送消息
                builder().setBolt("fielduvWithPlatform", new UvWithPlatformFieldsBolt(), 8).fieldsGrouping("recountUvWithPlatform",
                                                                                                           new Fields(
                                                                                                                      "guid"));
                // 计算app uv 入库
                builder().setBolt("dealappuvWithPlatform", new UvWithPlatformAppCountBolt(), 1).shuffleGrouping("fielduvWithPlatform");
                // 计算h5 uv 入库
                builder().setBolt("dealh5uvWithPlatform", new UvWithPlatformH5CountBolt(), 1).shuffleGrouping("fielduvWithPlatform");

                conf().setDebug(false);
                conf().setNumWorkers(10);
            }

            @Override
            public String help() {
                return "分平台（app，h5）统计UV";
            }
        });

        putTopo(new TopoGroup("RptRealtimeMontrTranx") {

            public void init(String[] args) {
                builder().setSpout(Constant.SQOUT_RPTREALTIMEMONTRTRANX,
                                   new TransDetlSpout(Constant.MQ_BI_RPT_REALTIME_MONTR_TRANX), 1);
                builder().setBolt(Constant.BOLT_RPT_REALTIME_MONTR_TRANX, new RptRealtimeMontrTranxBolt(), 1).shuffleGrouping(Constant.SQOUT_RPTREALTIMEMONTRTRANX);
                conf().setDebug(false);
                conf().setNumWorkers(4);
            }

            public String help() {
                return "全站成交口径明细(数据入Oracle)";
            }
        });

        // RPT_REALTIME_MONTR_Cancl 入库Oracle
        putTopo(new TopoGroup("RptRealtimeMontrCancl") {

            public void init(String[] args) {
                builder().setSpout(Constant.SQOUT_RPTMONTRCANCL,
                                   new OrderCancelSpout(Constant.MQ_BI_RPT_REALTIME_MONTR_CANCL), 1);
                builder().setBolt(Constant.BOLT_RPT_REALTIME_MONTR_CANCL, new RptRealtimeMontrCanclBolt(), 1).shuffleGrouping(Constant.SQOUT_RPTMONTRCANCL);
                conf().setDebug(false);
                conf().setNumWorkers(2);
            }

            public String help() {
                return "全站取消订单明细(数据入Oracle)";
            }
        });

        putTopo(new TopoGroup("SalesAmtCancl") {

            public void init(String[] args) {
                builder().setSpout(Constant.SPOUT_SALES_AMT_CANCL, new OrderCancelSpout(Constant.CID_SALESAMTCANCL), 1);
                builder().setBolt(Constant.BOLT_SALES_AMT_CANCL, new SalesAmtCanclBolt(), 1).shuffleGrouping(Constant.SPOUT_SALES_AMT_CANCL);
                conf().setDebug(false);
                conf().setNumWorkers(2);
            }

            public String help() {
                return "成交订单取消销售净额";
            }
        });

        // ---------------------以下topo仅供测试用--------------------------
        // TransactionSpout 加过滤条件测试
        putTopo(new TopoGroup("TransactionSpout_TEST") {

            @Override
            public void init(String[] args) {
                builder().setSpout("TransactionSpout_TEST", new TransactionSpout("TransactionSpout_TEST"), 1);
                builder().setSpout("OrderCancelSpout_TEST", new OrderCancelSpout("OrderCancelSpout_TEST"), 1);
                conf().setDebug(false);
                conf().setNumWorkers(2);
            }

            @Override
            public String help() {
                return "TransactionSpout & OrderCancelSpout 测试";
            }
        });

        // 成交口径测试
        putTopo(new TopoGroup("Trans_TEST") {

            @Override
            public void init(String[] args) {
                builder().setSpout(Constant.SPOUT_ONLINE_TRANS, new OnlineTransSpout("Trans_TEST"), 1);
                builder().setSpout(Constant.SPOUT_OFFLINE_TRANS, new OfflineTransSpout("Trans_TEST"), 1);
                builder().setSpout(Constant.SPOUT_ORDERCANCEL, new OrderCancelSpout("Trans_TEST"), 1);

                builder().setBolt(Constant.BOLT_TRANS_DETL, new TransDetlBolt(), 1).shuffleGrouping(Constant.SPOUT_ONLINE_TRANS).shuffleGrouping(Constant.SPOUT_OFFLINE_TRANS);
                builder().setBolt(Constant.BOLT_CANCEL_DETL, new CancelDetlBolt(), 1).shuffleGrouping(Constant.SPOUT_ORDERCANCEL);

                conf().setDebug(false);
                conf().setNumWorkers(5);
            }

            @Override
            public String help() {
                return "成交口径测试 测试";
            }
        });

        // 全站分钟顾客数,和每日销售净额、订单数
        putTopo(new TopoGroup("CustomersAndSalesTST") {

            @Override
            public void init(String[] args) {
                builder().setSpout(Constant.SPOUT_ONLINE_TRANS, new OnlineTransSpout(Constant.MQ_BI_ORDER_INFO), 1);
                builder().setSpout(Constant.SPOUT_OFFLINE_TRANS, new OfflineTransSpout(Constant.MQ_BI_ORDER_INFO), 1);

                // builder().setBolt("CustomerBolt", new CustomerBolt(), 1)
                // .shuffleGrouping(Constant.SPOUT_ONLINE_TRANS)
                // .shuffleGrouping(Constant.SPOUT_OFFLINE_TRANS);
                builder().setBolt(Constant.BOLT_SALE, new SalesBolt(), 1).shuffleGrouping(Constant.SPOUT_ONLINE_TRANS).shuffleGrouping(Constant.SPOUT_OFFLINE_TRANS);

                conf().setDebug(false);
                conf().setNumWorkers(3);
            }

            @Override
            public String help() {
                return "全站分钟顾客数,和每日销售净额、订单数";
            }
        });

        // PV 测试
        putTopo(new TopoGroup("PV") {

            @Override
            public void init(String[] args) {
                builder().setSpout("TrackSpout", new TrackBatchSpout(Constant.CID_PV), 3);
                builder().setBolt("PV", new PVIncBolt(), 5).shuffleGrouping("TrackSpout");

                conf().setDebug(false);
                conf().setNumWorkers(8);
            }

            @Override
            public String help() {
                return "PV 测试";
            }
        });

        // 神针 流量
        putTopo(new TopoGroup("Needle") {	
        	
        	 @Override
             public void init(String[] args) {
				builder().setSpout("spout", new KafkaTrackerSpout("test"), 1);
		    	builder().setBolt("filter", new UVShuffleBolt(), 2).shuffleGrouping("spout");
				builder().setBolt("cache", new UvCacheBolt(), 5).fieldsGrouping("filter", new Fields("guid"));
//				
		    	conf().setDebug(false);
                conf().setNumWorkers(10);

            }

            @Override
            public String help() {
                // TODO Auto-generated method stub
                return "神针 流量";
            }
        });
    }

    public static void submit(String[] args) throws AlreadyAliveException, InvalidTopologyException, NotAliveException,
                                            TException, InterruptedException {
        if (args == null || args.length < 2 || StringUtils.isEmpty(args[0])) {
            printUsage();
        } else {
            String cmd = args[0].toLowerCase();
            String gpName = args[1];
            TopoGroup tg = tops.get(gpName);

            if (tg == null) {
                printUsage();
            }
            
            String[] init = Arrays.copyOfRange(args, 1, args.length);
            tg.init(init);
            
            if ("start".equals(cmd)) {
                StormSubmitter.submitTopology(tg.getName(), tg.conf(), tg.builder().createTopology());
            } else if ("kill".equals(cmd)) {
                if (confirm("Kill Topo `" + tg.getName() + "`")) {
                    Map conf = Utils.readStormConfig();
                    Client client = NimbusClient.getConfiguredClient(conf).getClient();
                    client.killTopology(tg.getName());
                    System.out.println(new SimpleDateFormat("HH:mm:ss,SSS").format(Calendar.getInstance().getTime())
                                       + " |-INFO in " + TopoSubmitter.class.getCanonicalName() + " - Topology `"
                                       + tg.getName() + "` has been killed");
                }
            } else if ("restart".equals(cmd)) {
                if (confirm("Restart Topo `" + tg.getName() + "`")) {
                    Map conf = Utils.readStormConfig();
                    Client client = NimbusClient.getConfiguredClient(conf).getClient();
                    client.killTopology(tg.getName());

                    System.out.println(new SimpleDateFormat("HH:mm:ss,SSS").format(Calendar.getInstance().getTime())
                                       + " |-INFO in " + TopoSubmitter.class.getCanonicalName() + " - Topology `"
                                       + tg.getName() + "` has been killed");
                    System.out.println(new SimpleDateFormat("HH:mm:ss,SSS").format(Calendar.getInstance().getTime())
                                       + " |-INFO in " + TopoSubmitter.class.getCanonicalName()
                                       + " - Restarting topology `" + tg.getName() + "`");

                    Iterator<TopologySummary> topoItr = client.getClusterInfo().get_topologies_iterator();
                    String topoId = null;
                    while (topoItr.hasNext()) {
                        TopologySummary topo = topoItr.next();
                        if (topo.get_name().equals(tg.getName())) {
                            topoId = topo.get_id();
                            break;
                        }
                    }
                    if (topoId != null) {
                        while (true) {
                            try {
                                client.getTopology(topoId);
                            } catch (NotAliveException e) {
                                break;
                            }
                            System.out.print("#");
                            Thread.sleep(RESTART_WAIT_TIME);
                        }
                        System.out.println();
                    }
                    
                    StormSubmitter.submitTopology(tg.getName(), tg.conf(), tg.builder().createTopology());
                }
            } else if ("recovery".equals(cmd)) {
                tg.recovery(args);
            } else if ("monitor".equals(cmd)) {
                tg.monitor(args);
            } else if ("local".equals(cmd)) {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(tg.getName(), tg.conf(), tg.builder().createTopology());
            } else if ("debug".equals(cmd)) {
                Debug.DEBUG = true;
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(tg.getName(), tg.conf(), tg.builder().createTopology());
            } else {
                System.out.println("Unknown COMMAND `" + args[0] + "`");
                printUsage();
            }
        }
    }

    public static void submit(String gpName) throws AlreadyAliveException, InvalidTopologyException, NotAliveException,
                                            TException, InterruptedException {
        submit(new String[] { gpName });
    }

    public static void printUsage() {
        System.out.printf("%s\n", "Usage: com.yihaodian.bi.storm.base.topo.Main COMMAND TopoID");
        System.out.printf("\n%s\n", "COMMAND is one of:");
        System.out.printf("  %-30s%s\n", "start", "run a new job");
        System.out.printf("  %-30s%s\n", "kill", "kill a running job");
        System.out.printf("  %-30s%s\n", "restart", "restart a running job");
        System.out.printf("  %-30s%s\n", "local", "run a job in local mode");
        System.out.printf("  %-30s%s\n", "recovery", "run a job in local mode");
        System.out.printf("  %-30s%s\n", "monitor", "run a job in local mode");
        System.out.printf("\n%s\n", "TopoID is one of:");
        for (Map.Entry<String, TopoGroup> entry : tops.entrySet()) {
            System.out.printf("  %-30s%s\n", entry.getKey(), entry.getValue().help());
        }
    }
}
