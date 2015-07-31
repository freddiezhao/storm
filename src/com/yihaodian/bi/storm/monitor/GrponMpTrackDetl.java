package com.yihaodian.bi.storm.monitor;

import java.sql.ResultSet;
import java.sql.Statement;

public class GrponMpTrackDetl extends BaseMonitor{
	
	public static final String TOPIC="[STORM监控][团闪小口径流量]";
	
	@Override
	public void monitor(String[] args) throws Exception{
		Statement st = oracleCon.createStatement();
		String sql="select count(*) from rpt.rpt_realtime_gm_track_detl where date_time_id >sysdate-2/(24*60)";
		
		ResultSet rs=st.executeQuery(sql);
		while(rs.next()){
			long number=rs.getLong(1);
			msg("检查rpt.rpt_realtime_gm_track_detl表格在过去的2分钟内是否有数据");
			if(number==0){
				String msg="rpt.rpt_realtime_gm_track_detl表数据出现异常";
				sendMessageByCache(msg,TOPIC);
			}
		}
		
	}
	
	@Override
	public String getMonitorName(){
		return "GrponMpTrackDetl-monitor";
	}
	
	public static void main(String[] args) throws Exception{
		GrponMpTrackDetl gt=new GrponMpTrackDetl();
		gt.doMonitor(args);
	}
	@Override
	public void help(){
		msg("团闪小口径流量监控");
		msg("检查截止到当前时间两分钟内的数据量，若为空则报警");
	}

}
