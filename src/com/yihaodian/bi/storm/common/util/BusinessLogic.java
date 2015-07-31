package com.yihaodian.bi.storm.common.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.cached.CMap;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.BaseDao;
import com.yihaodian.bi.storm.common.model.JumpMQOrderItemVo;
import com.yihaodian.bi.storm.common.model.JumpMQOrderVo;
import com.yihaodian.bi.storm.common.model.TrackerVo;

public class BusinessLogic {

    private static final Logger LOG                         = LoggerFactory.getLogger(BusinessLogic.class);

    private Connection          con                         = null;
    private static long         YHD_SELF_IDS_LAST_FLUSHTIME = 0;
    private static String       YHD_SELF_IDS_STRING         = null;
    private static Map<Long, Boolean>  GIFT_CARD_PROD_LIST  = null;

    public BusinessLogic(Connection con){
        this.con = con;
    }

    /**
     * 固定时间刷新
     */
    public boolean flushYHDSelfIds() {
        // 十分钟刷新一次 一号点自营 IDs 列表
        if (System.currentTimeMillis() - YHD_SELF_IDS_LAST_FLUSHTIME > 600000) {
            String newIds = innerGetYHDSelfIDs();
            YHD_SELF_IDS_LAST_FLUSHTIME = System.currentTimeMillis();
            if (!StringUtils.isEmpty(newIds)) {
                YHD_SELF_IDS_STRING = newIds;
                LOG.info("Flush OK YHDs Is [" + newIds + "] ");
                return true;
            } else {
                LOG.error("Flush ERROR YHDs Is [" + newIds + "] ");
            }
        }
        return false;
    }

    public String getYHDSelfIDs(){
        return YHD_SELF_IDS_STRING;
    }
    
    // 获取自营商家 ID 列表，固化在 内存中用于判断是自营商家还是商城商家
    private String innerGetYHDSelfIDs() {
        Statement st = null;
        try {
            if (con == null || con.isClosed()) {
            	throw new RuntimeException("Connection is Closed");
            }
            st = con.createStatement();
            ResultSet rs = st.executeQuery("select t.mrchnt_id as id from dw.dim_mrchnt t where t.cur_flag = 1 and t.biz_unit = 1");
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(",").append(rs.getString("id")).append(",");
            }
            return sb.toString();
        } catch (Exception e1) {
            LOG.error("获取自营商家 IDs ERROR", e1);
        } finally {
            try {
                st.close();
            } catch (SQLException e) {
                LOG.error("获取自营商家 IDs ERROR", e);
            }
        }
        return null;
    }

    /**
     * 检查是否是YHD自营
     * 
     * @param merId 商家ID
     * @return
     */
    public boolean isYHDSelf(long merId) {
        flushYHDSelfIds();
        if (YHD_SELF_IDS_STRING != null) {
            if (YHD_SELF_IDS_STRING.indexOf("," + merId + ",") > -1) {
                LOG.info("merchanid" + merId + " is yhdself");
                return true;
            } else {
                LOG.info("merchanid" + merId + " is nonyhdself");
                return false;
            }
        }
        return false;
    }
    
    public static Date orderDate(JumpMQOrderVo jumpMQOrderVo) {
    	if (Constant.ONLINE_PAY.equals(jumpMQOrderVo.getPaymentCategory()))
    		return jumpMQOrderVo.getOrderPaymentConfirmDate();
    	
    	return jumpMQOrderVo.getOrderCreateTime();
    }
    
    // 如果订单净额和积分都为零（则不算成交）
    public static boolean isTransation(JumpMQOrderItemVo soItem) {
    	if (soItem.getOrderItemAmount().compareTo(new BigDecimal("0"))==0 
				&& soItem.getIntegral().equals(new Integer(0))) {
    		return false;
    	}
    	
    	return true;
    }
    
    // 是否为礼品卡
    public static boolean isGiftCard(Long prodId) {
    	if (prodId != null 
    			&& GIFT_CARD_PROD_LIST.get(prodId) != null) {
    		return true;
    	}
    	
    	return false;
    }
    
    // 初始化礼品卡产品表
    public static void initGiftCardList() {
    	System.out.println("Initializing GIFT CARD List.....");
    	GIFT_CARD_PROD_LIST = new HashMap<Long, Boolean>();
    	Connection conn = new OracleConnection().getConnection();
    	
    	 Statement st = null;
         try {
             if (conn == null || conn.isClosed()) {
             	throw new RuntimeException("Connection is Closed");
             }
             st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT t4.prod_id\n" + 
						             		"FROM   dw.dim_prod t4\n" + 
						             		"WHERE  t4.prod_type IN (4, 7)\n" + 
						             		"AND    t4.cur_flag = 1");
             
             while (rs.next()) {
            	 LOG.info("prodId:" + rs.getLong(1));
            	 GIFT_CARD_PROD_LIST.put(rs.getLong(1), true);
             }
         } catch (Exception e) {
             LOG.error("获取礼品卡 IDs ERROR", e);
         } finally {
             try {
                 st.close();
                 conn.close();
             } catch (SQLException e) {
                 LOG.error("获取获取礼品卡 IDs ERROR", e);
             }
         }
    }
    
	/**
	 * 判断平台类型
	 * 
	 * TODO 确定类型 0 app 1 h5
	 * @param url
	 * @param container
	 * @return 
	 */
	public static String pltmType(TrackerVo t){
		String platform = t.getPlatform();
		String container= t.getContainer();
		String url = t.getUrl();
		
		if (platform != null) {
			platform = platform.toLowerCase();
		}
		if (platform != null && container != null) {
			if(container.equals("yhdapp") && 
					(platform.indexOf("iossystem") != -1 || platform.endsWith("iphone"))){
				 return "1";
			}else if(container.equals("yhdapp") && 
					(platform.indexOf("ipadsystem") != -1 || platform.endsWith("ipad"))){
				 return "2";
			}else if(container.equals("yhdapp") && 
					(platform.indexOf("androidsystem") != -1 || platform.endsWith("android"))){
				 return "3";
			}else if((container.equals("H5") || (!container.equals("yhd-im-pc") && !container.isEmpty()) 
					&& (platform.indexOf("androidsystem") != -1 || platform.indexOf("iossystem") != -1))) {
				 return "4";
			}
		}
		
		if (platform != null && url != null) {
			if (!url.isEmpty() && (url.indexOf("http://m.") == -1) 
				&& (platform.indexOf("iossystem") != -1 || platform.indexOf("ipadsystem") != -1  || platform.indexOf("androidsystem") != -1
					|| platform.endsWith("iphone") || platform.endsWith("ipad") || platform.endsWith("android")))
			{
				return "4";
			}
			else if (url.startsWith("http://m.yhd.com") 
					|| (url.startsWith("http://m.yihaodian.com") && url.indexOf("/wm/") != -1)
					|| url.indexOf(".m.yhd.com") != -1) {
				return "4";
			}
		}
				
		return "-1";
	}
	
	/**
	 * 获得渠道
	 * 
	 * @param dao
	 * @param t
	 * @return
	 * @throws IOException
	 */
	public static String channel(BaseDao dao, TrackerVo t) throws IOException {
		String tracker_u = t.getTracker_u();
		if (tracker_u != null) {
			String res = Get.getChannel(dao, tracker_u);
			if (res == null) {
				return Constant.UNKNOWN;
			}
			else {
				return res;
			}
		}
		String referer = t.getReferer();
		if (referer != null && !referer.isEmpty()) {
			if (referer.indexOf(".baidu.com/") != -1 || referer.startsWith("http://so.360.cn/")
					|| referer.startsWith("http://www.so.com/") || referer.startsWith("http://m.so.com/")
					|| referer.startsWith("http://www.haosou.com/") || referer.startsWith("http://www.google.")
					|| referer.startsWith("https://www.google.") || referer.indexOf(".soso.com/") != -1
					|| referer.startsWith("http://www.sogou.com/") || referer.startsWith("http://gouwu.youdao.com/search")
					|| referer.indexOf(".bing.com/") != -1 || referer.startsWith("http://www.youdao.com/search?q=")
					|| referer.indexOf(".yahoo.com/") != -1 || referer.indexOf(".yahoo.cn/") != -1
					|| referer.indexOf("http://glb.uc.cn/") != -1 || referer.indexOf(".sm.cn/") != -1 
					|| referer.startsWith("http://www.yhd.com/s-theme/") || referer.startsWith("http://www.yhd.com/S-theme/")
					|| referer.startsWith("http://www.yhd.com/marketing/hs-")){
				return Constant.SEO;
			}
		}
		else {
			return Constant.DIRECT;
		}
		
		return Constant.UNKNOWN;
	}
    
	/**
	 * 获得渠道 查cache
	 * 
	 * @param dao
	 * @param t
	 * @return
	 * @throws Exception 
	 */
	public static String channel(CMap cache, TrackerVo t) throws Exception {
		String tracker_u = t.getTracker_u();
		if (tracker_u != null) {
			String res = cache.get(tracker_u);
			if (res == null) {
				return Constant.UNKNOWN;
			}
			else {
				return res;
			}
		}
		String referer = t.getReferer();
		if (referer != null && !referer.isEmpty()) {
			if (referer.indexOf(".baidu.com/") != -1 || referer.startsWith("http://so.360.cn/")
					|| referer.startsWith("http://www.so.com/") || referer.startsWith("http://m.so.com/")
					|| referer.startsWith("http://www.haosou.com/") || referer.startsWith("http://www.google.")
					|| referer.startsWith("https://www.google.") || referer.indexOf(".soso.com/") != -1
					|| referer.startsWith("http://www.sogou.com/") || referer.startsWith("http://gouwu.youdao.com/search")
					|| referer.indexOf(".bing.com/") != -1 || referer.startsWith("http://www.youdao.com/search?q=")
					|| referer.indexOf(".yahoo.com/") != -1 || referer.indexOf(".yahoo.cn/") != -1
					|| referer.indexOf("http://glb.uc.cn/") != -1 || referer.indexOf(".sm.cn/") != -1 
					|| referer.startsWith("http://www.yhd.com/s-theme/") || referer.startsWith("http://www.yhd.com/S-theme/")
					|| referer.startsWith("http://www.yhd.com/marketing/hs-")){
				return Constant.SEO;
			}
		}
		else {
			return Constant.DIRECT;
		}
		
		return Constant.UNKNOWN;
	}
	
    public static void main(String args[]) throws InterruptedException {
    	Thread.sleep(10000);
    	if (BusinessLogic.isGiftCard(4039039L))
    		System.out.println("hello");
    	
    	if (BusinessLogic.isGiftCard(4039039L))
    		System.out.println("world");
    }
}
