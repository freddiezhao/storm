package com.yihaodian.bi.storm.monitor;

import java.sql.ResultSet;
import java.sql.Statement;

public class GrponMpOrdrDetl extends BaseMonitor {
	
	public static final String TOPIC="[STORM监控][团闪订单明细]";
     
	@Override
	public void monitor(String[] args) throws Exception {
		
		Statement st = oracleCon.createStatement();
		String sql="select count(*) from rpt.rpt_realtime_gm_ordr_detl where date_time_id >sysdate-2/(24*60)";
		ResultSet rs=st.executeQuery(sql);
		
		while(rs.next()){
			long number=rs.getLong(1);
			msg("检查rpt.rpt_realtime_gm_ordr_detl表在过去的两分钟内是否有数据");
			if (number==0){
				String msg="rpt.rpt_realtime_gm_ordr_detl表格数据出现异常";
				sendMessageByCache(msg,TOPIC);
			}			
		}
	}

	@Override
	public String getMonitorName() {
		return "GrponMpOrdrDetl-monitor";
	}
	
	
	public static void main(String[] args) throws Exception{
		GrponMpOrdrDetl g=new GrponMpOrdrDetl();
		g.doMonitor(args);
	}
	@Override
	public void help(){
		msg("团闪订单明细监控");
		msg("检查截止到当前时间两分钟内的数据量，若为空则报警");
	}

}
