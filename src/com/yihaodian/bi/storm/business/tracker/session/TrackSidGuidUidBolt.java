package com.yihaodian.bi.storm.business.tracker.session;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.storm.common.model.TrackerVo;
import com.yihaodian.bi.storm.common.util.Constant;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TrackSidGuidUidBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2165123436356633556L;
	private static final Logger logger= LoggerFactory.getLogger(TrackSidGuidUidBolt.class);
	private OutputCollector _collector;
	private DBConnection _dbConnection;
	private Connection _connection;
	
	private static final String SCHEMA = "rpt";
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override @SuppressWarnings("unchecked")
	public void execute(Tuple input) {
		List<TrackerVo> trackerList = (ArrayList<TrackerVo>)input.getValue(0);
		
		if (trackerList != null && !trackerList.isEmpty()) {
			for (TrackerVo tracker : trackerList) {
				if (tracker != null) { 
					String dateTimeID = DateUtil.getCountDate(tracker.getTrack_time(), DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
					String uid = tracker.getEnd_user_id();
					String sid = tracker.getSession_id();
					String guid= tracker.getGu_id();
					
//					logger.info("Dateid: " + dateTimeID + "|" +
//							    "uid   : " + uid    + "|" +
//							    "sid   : " + sid    + "|" +
//							    "guid  : " + guid   + "|" );
					
					if (dateTimeID != null && !dateTimeID.isEmpty() &&
							   uid != null && !uid.isEmpty() &&
							  (sid != null && !sid.isEmpty() || guid !=null && guid.isEmpty()) ) {
						String updateTime = DateUtil.getFmtDate(null, DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
						insertToOracle(_connection, dateTimeID, uid, sid, guid, updateTime);
					}
				}
			}
		}	
		_collector.ack(input);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		_dbConnection = new OracleConnection();
	    _connection = _dbConnection.getConnection();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * 
	 * @param con
	 * @param date_time_id
	 * @param uid
	 * @param sid
	 * @param guid
	 * @param update_time
	 */
	private static void insertToOracle(Connection con, String date_time_id, String uid, String sid, String guid, String update_time){
		Statement statement = null;
		
		try {
			statement = con.createStatement();
			
			String sql = 
				"insert into " + SCHEMA + "." + Constant.TABLE_TRACK_SESSION_UID_DETL + "(DATE_ID, DATE_TIME_ID, SESSION_ID, GUID, END_USER_ID, UPDATE_TIME) " +
		  		"values (trunc(to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss')), to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss'), '" +  sid + "', '" + guid + "', '" + uid + "', " + "to_date('" + update_time + "','yyyy-MM-dd hh24:mi:ss'))";
			statement.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.info("Insert to Oracle failed! Probably caused by unique index!");
		} finally {
			try {
				statement.close() ;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
