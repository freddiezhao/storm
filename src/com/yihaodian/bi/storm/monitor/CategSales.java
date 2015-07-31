package com.yihaodian.bi.storm.monitor;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;

import org.apache.commons.lang.StringUtils;

import com.yihaodian.bi.common.util.DateUtil;

public class CategSales extends BaseMonitor {

    public static final int ERROR_COUNT_DEFAULT = 20;
    public static final int ERROR_COUNT_MAX     = 50;
    private static String TOPIC = "[STORM监控][监控室大屏]";
    public static final int CHECK_TIME_AGO      = 20;

    @Override
    protected void monitor(String[] args) throws Exception {
        int checkErrorCount = ERROR_COUNT_DEFAULT;
        if (args.length > 0) {
            checkErrorCount = Integer.parseInt(args[0]);
        }
        if (checkErrorCount > ERROR_COUNT_MAX) {
            checkErrorCount = ERROR_COUNT_MAX;
        }

        Statement st = oracleCon.createStatement();
        // 抽样 只取部分常用有效的 ID
        ResultSet rs = st.executeQuery("select * from dw.dim_categ_lvl1 where cur_flag=1 and PROD_DEPT_ID<>4");

        long cd = System.currentTimeMillis() - (60 * 1 * 1000); // a minute ago

        long cd7ago = cd - 60 * 60 * 24 * 7 * 1000 - (60 * 1 * 1000);
        Date d = new Date(cd); // 当前时间的前一分钟
        Date d7ago = new Date(cd7ago); // 七天以前

        String strCurrentHour = DateUtil.YYYYMMDDHH.format(d);

        int errorCount = 0;
        while (rs.next()) {
            String cateId = rs.getString("CATEG_LVL1_ID");
            String cateName = rs.getString("CATEG_LVL1_NAME");
            try {
                errorCount += dataQualityCheck(d, cateName, cateId);
                errorCount += dataQualityCheck(d7ago, cateName, cateId);
            } catch (Exception e) {
                msg(e.getMessage());
            }
            if (errorCount >= ERROR_COUNT_MAX) {
                break;
            }
        }
        // 一个小时只N次以上问题才发送报警 N = intTime
        String time = getCache(strCurrentHour);
        int intTime = 0;
        if (time != null) {
            intTime = Integer.parseInt(time);
        }
        intTime = errorCount + intTime;
        // 一个小时30次才发送报警，最少10分钟现问题
        msg("此次总共发现错误 " + errorCount);
        if (intTime >= checkErrorCount) {
            sendMessageByCache("单类目累计一小时内发生超过 " + intTime + " 次异常，请及时观察",TOPIC+"[一级类目销售净额]");
            // 清空缓存
            putCache(strCurrentHour, "0");
        } else {
            putCache(strCurrentHour, String.valueOf(intTime));
        }
    }

    private int dataQualityCheck(Date d, String cateName, String cateId) throws Exception {
        String key = DateUtil.YYYYMMDDHHMM.format(d) + "_" + cateId;
        int errorCount = 0;
        String value = getHBase().getOrderDao().getValue("bi_monitor_categ_rslt", key);

        if (StringUtils.isEmpty(value)) {
            // 如果出现没有值则停止
            msg(key + ":" + cateName + " 值是空, value = " + value + " , 错误 + 1");
            errorCount++;
        }
        Date dAgo = new Date(d.getTime() - (60 * CHECK_TIME_AGO * 1000)); // 20 minute ago
        String keyAgo = DateUtil.YYYYMMDDHHMM.format(dAgo) + "_" + cateId;
        String valueAgo = getHBase().getOrderDao().getValue("bi_monitor_categ_rslt", keyAgo);
        if (StringUtils.isEmpty(valueAgo)) {
            // 如果出现没有值则停止
            msg(keyAgo + ":" + cateName + " 值是空, value = " + valueAgo + " , 错误 + 1");
            errorCount++;
        }

        Double dV = Double.parseDouble(value);
        Double dVago = Double.parseDouble(valueAgo);

        if (dVago >= dV) {
            msg(key + ":" + cateName + "相比较" + CHECK_TIME_AGO + "分钟之前无增长, 错误 + 1");
            errorCount++;
        }
        return errorCount;
    }

    @Override
    public String getMonitorName() {
        return "cate-sales";
    }

    public static void main(String[] args) throws Exception {
        CategSales c = new CategSales();
        c.doMonitor(args);
    }

    @Override
    public void help() {
        msg("一级品类销售净额监控");
        msg("\t1. 依次检查当前时间和同比时间的前1分钟1级品类的销售净额是否为空，如果为空 则错误总数加1");
        msg("\t2. 依次检查当前时间的前1分钟1级品类的销售净额相比较20分钟前是否有增长，如果不增长则 错误总数加1");
        msg("报警规则： 1小时内错误累加到指定阈值则报警，需要手动调节为合理值（如果这1分钟的监控中17个品类全部报错总数相加大于指定值则直接报警）");
        msg("参数列表： [电话号码列表]\t [一小时内出现问题的概率，数字1-50之间]");
    }

}
