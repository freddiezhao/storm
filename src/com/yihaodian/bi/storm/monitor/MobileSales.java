package com.yihaodian.bi.storm.monitor;

import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Calendar;

/**
 * 手机报表
 * 
 * @author zhaoheng Jun 9, 2015 2:32:24 PM
 */
public class MobileSales extends BaseMonitor {
	public static final String TOPIC = "[STORM监控][手机报表]";
    public static final String  ORACLE_TABLE = "rpt_realtime_mp_amt_o";
    public static final String  STORM_TABLE  = "rpt_realtime_mp_amt_s";
//    public static final String  STORM_CANCL_TABLE="rpt_realtime_mp_amt_cancl_s";
    private static NumberFormat NF           = NumberFormat.getInstance();
    private static double       PERCENT      = 0.09D;
    
    static {
        NF.setGroupingUsed(false);
    }

    @Override
    public void monitor(String[] args) throws Exception {
        String sysdate = "SYSDATE";
        String calls = "";
        if (args.length >= 1) {
            PERCENT = Double.parseDouble(args[0]);
        }
        if (args.length >= 2) {
            sysdate = args[1];
        }
        monitorTable(STORM_TABLE, sysdate, calls);
    }

    protected void monitorTable(String table, String sysdate, String calls) throws Exception {
        Statement st = oracleCon.createStatement();
        Calendar c = Calendar.getInstance();
        int cc = c.get(Calendar.HOUR_OF_DAY);

        
        String sql ="SELECT storm.date_time_id,\n" + 
        		"       storm.ordr_amt AS storm_amt,\n" + 
        		"       oracle.ordr_amt AS oracle_amt,\n" + 
        		"       storm.ordr_amt - oracle.ordr_amt AS diff_amt\n" + 
        		"\n" + 
        		"FROM   (SELECT CASE\n" + 
        		"                  WHEN substr(min_date_time_id,\n" + 
        		"                              length(min_date_time_id) - 4,\n" + 
        		"                              length(min_date_time_id)) <> '00:00' THEN\n" + 
        		"                   REPLACE(date_time_id,\n" + 
        		"                           substr(min_date_time_id,\n" + 
        		"                                  length(min_date_time_id) - 4,\n" + 
        		"                                  length(min_date_time_id)),\n" + 
        		"                           '00:00')\n" + 
        		"                  ELSE\n" + 
        		"                   date_time_id\n" + 
        		"               END AS date_time_id,\n" + 
        		"               SUM(y.ordr_amt) ordr_amt\n" + 
        		"        \n" + 
        		"        FROM   (SELECT to_char(date_time_id, 'yyyy-mm-dd hh24:mi:ss') AS date_time_id,\n" + 
        		"                       x.biz_unit,\n" + 
        		"                       x.pltfm_lvl1_id,\n" + 
        		"                       x.prmtn_rule_type,\n" + 
        		"                       x.ordr_amt,\n" + 
        		"                       MIN(to_char(date_time_id, 'yyyy-mm-dd hh24:mi:ss')) OVER(PARTITION BY to_char(x.date_time_id, 'yyyy-mm-dd hh24')) AS min_date_time_id\n" + 
        		"                FROM   rpt."+table+" x\n" + 
        		"                WHERE  x.date_time_id >= trunc("+sysdate+") " +
        		"                      and x.date_time_id<trunc("+sysdate+")+1) y\n" + 
        		"        \n" + 
        		"        WHERE  y.date_time_id = y.min_date_time_id\n" + 
        		"        GROUP  BY (CASE\n" + 
        		"                     WHEN substr(min_date_time_id,\n" + 
        		"                                 length(min_date_time_id) - 4,\n" + 
        		"                                 length(min_date_time_id)) <> '00:00' THEN\n" + 
        		"                      REPLACE(date_time_id,\n" + 
        		"                              substr(min_date_time_id,\n" + 
        		"                                     length(min_date_time_id) - 4,\n" + 
        		"                                     length(min_date_time_id)),\n" + 
        		"                              '00:00')\n" + 
        		"                     ELSE\n" + 
        		"                      date_time_id\n" + 
        		"                  END)\n" + 
        		"        ORDER  BY date_time_id) storm\n" + 
        		"\n" + 
        		"INNER  JOIN (SELECT CASE\n" + 
        		"                       WHEN substr(min_date_time_id,\n" + 
        		"                                   length(min_date_time_id) - 4,\n" + 
        		"                                   length(min_date_time_id)) <> '00:00' THEN\n" + 
        		"                        REPLACE(date_time_id,\n" + 
        		"                                substr(min_date_time_id,\n" + 
        		"                                       length(min_date_time_id) - 4,\n" + 
        		"                                       length(min_date_time_id)),\n" + 
        		"                                '00:00')\n" + 
        		"                       ELSE\n" + 
        		"                        date_time_id\n" + 
        		"                    END AS date_time_id,\n" + 
        		"                    SUM(y.ordr_amt) ordr_amt\n" + 
        		"             \n" + 
        		"             FROM   (SELECT to_char(date_time_id, 'yyyy-mm-dd hh24:mi:ss') AS date_time_id,\n" + 
        		"                            x.biz_unit,\n" + 
        		"                            x.pltfm_lvl1_id,\n" + 
        		"                            x.prmtn_rule_type,\n" + 
        		"                            x.ordr_amt,\n" + 
        		"                            MIN(to_char(date_time_id, 'yyyy-mm-dd hh24:mi:ss')) OVER(PARTITION BY to_char(x.date_time_id, 'yyyy-mm-dd hh24')) AS min_date_time_id\n" + 
        		"                     FROM   rpt."+ORACLE_TABLE+" x\n" + 
        		"                     WHERE  x.date_time_id >= trunc("+sysdate+") " +
        		"                           and x.date_time_id<trunc("+sysdate+")+1) y\n" + 
        		"             \n" + 
        		"             WHERE  y.date_time_id = y.min_date_time_id\n" + 
        		"             GROUP  BY (CASE\n" + 
        		"                          WHEN substr(min_date_time_id,\n" + 
        		"                                      length(min_date_time_id) - 4,\n" + 
        		"                                      length(min_date_time_id)) <> '00:00' THEN\n" + 
        		"                           REPLACE(date_time_id,\n" + 
        		"                                   substr(min_date_time_id,\n" + 
        		"                                          length(min_date_time_id) - 4,\n" + 
        		"                                          length(min_date_time_id)),\n" + 
        		"                                   '00:00')\n" + 
        		"                          ELSE\n" + 
        		"                           date_time_id\n" + 
        		"                       END)\n" + 
        		"             ORDER  BY date_time_id) oracle\n" + 
        		"ON     storm.date_time_id = oracle.date_time_id\n" + 
        		"";
        msg(sql);
        ResultSet rs = st.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
            String dt = rs.getString(1);
            Double sValue = rs.getDouble(2);
            Double tValue = rs.getDouble(3);
            Double diff = rs.getDouble(4);
            try {
                String key = table + " " + dt;
                msg(key + "\t" + NF.format(sValue) + "\t" + NF.format(tValue) + "\t" + diff + "\t" 
                		+ NF.format(Math.abs(sValue - tValue) / tValue) + "\n");
                if (table.equals(STORM_TABLE)) {
                    stormRule(dt, sValue, tValue, diff);
                } 
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (count < cc) {
            String msg = "";
            if (table.equals(STORM_TABLE)) {
                msg = table + " Storm 应产出[" + cc + "]小时数据，但实际只产出[" + count + "]小时，请验证每小时数据对比差异是否过大、及Storm是否正常";
            } 
            sendMessageByCache(msg,TOPIC);
        }
    }

    protected void stormRule(String dt, Double storm, Double ora, Double diff) throws Exception {
        if (Math.abs(storm - ora) / ora > PERCENT) {
        	double d = (storm - ora) / ora * 100;
			DecimalFormat df = new DecimalFormat("######.##");
			String percent = df.format(d);
            String msg =  "在" + dt + " Storm成交净额=" + NF.format(storm) + ",Oracle成交净额=" + NF.format(ora)+",相差" + percent + "%";
            msg(getCache("storm-" + dt));
            if(getCache("storm-" + dt) == null){
                sendMessageByCache(msg,TOPIC);
            	putCache("storm-" + dt, "true");
            }
            return;
        }
    }

    @Override
    public String getMonitorName() {
        return "mobile-monitor";
    }


    public static void main(String[] args) throws Exception {
        MobileSales m = new MobileSales();
        m.doMonitor(args);
    }

    @Override
    public void help() {
    	msg("手机报表 销售净额监控");
		msg("\t1. 依次检查截止当前时间的小时的销售净额，如果 Storm 应产出的小时数据不等于实际产出数据，则报警");
		msg("销售净额报警规则：ABS ( Storm销售净额 – Oracle销售净额 ) / Oracle  > 9%则直接报警）");
    }
}
