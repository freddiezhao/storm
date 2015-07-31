package com.yihaodian.bi.storm.business.tracker.groupon;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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

public class TrackMpDetlBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -733912009489633973L;
	private static final Logger logger= LoggerFactory.getLogger(TrackMpDetlBolt.class);
	private OutputCollector _collector;
	private DBConnection _dbConnection;
	private Connection _connection;
	
	private static final Integer mpFlag = 11;
	private static final String SCHEMA = "rpt";
	private static final Integer MP_PC = 201;
	private static final Integer MP_H5 = 202;
	private static final Integer MP_APP = 203;
	private static final Integer UNKNOWN = -999999;
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		String streamId = input.getSourceStreamId();
		TrackerVo tracker = (TrackerVo)input.getValueByField("tracker");
		
		if (tracker != null) { 
			String dateTimeId = DateUtil.getCountDate(tracker.getTrack_time(), DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
			String uid = tracker.getEnd_user_id();
			String sid = tracker.getSession_id();
			String guid= tracker.getGu_id();
			String prodId = tracker.getProduct_id();
//			String url = tracker.getUrl();
			Integer trackSource = UNKNOWN;
			if (streamId.equals("mp_pc"))
				trackSource = MP_PC;
			else if (streamId.equals("mp_h5"))
				trackSource = MP_H5;
			else if (streamId.equals("mp_app"))
				trackSource = MP_APP;
			
			String pmId = tracker.getExt_field7();
			if(pmId==null||"".equals(pmId)) {
				pmId=tracker.getPageValue();
			}
//			logger.info("Dateid: " + dateTimeId + "|" +
//					    "uid   : " + uid    + "|" +
//					    "sid   : " + sid    + "|" +
//					    "guid  : " + guid   + "|" + 
//					    "prodID: " + prodId + "|" +
//					    "pmID  : " + pmId   + "|" +
//					    "source: " + trackSource + "|" +
//					    "url   : " + url);
			
//			if (dateTimeId != null && !dateTimeId.isEmpty() &&
//					prodId != null && !prodId.isEmpty() || pmId != null && !pmId.isEmpty()) {
				String updateTime = DateUtil.getFmtDate(null, DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
				insertToOracle(_connection, dateTimeId, uid, sid, guid, prodId, pmId, mpFlag, trackSource, updateTime);
//			}
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
	 * @param prodID
	 * @param flag
	 * @param update_time
	 */
	private static void insertToOracle(Connection con, String date_time_id, String uid, String sid, String guid, String prodId, String pmId, Integer flag, Integer trackSource, String update_time){
		Statement statement = null;
		
		try {
			statement = con.createStatement();
			
//			if (uid == null || uid.isEmpty()) uid = UNKNOWN.toString();
			
			String sql = 
				"insert into " + SCHEMA + "." + Constant.TABLE_TRACK_GROUPON_MP_DETL + "(DATE_ID, DATE_TIME_ID, END_USER_ID, SESSION_ID, GUID, PROD_ID, PM_ID, PAGE_TYPE_ID, TRACK_SOURCE, UPDATE_TIME) " +
		  		"values (trunc(to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss')), to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss'), '" +  uid + "', '" + sid + "', '" + guid + "', '" + prodId + "', '" + pmId + "', '" + flag + "', '" + trackSource +  "', " + "to_date('" + update_time + "','yyyy-MM-dd hh24:mi:ss'))";
			statement.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.info("Insert to Oracle failed!");
		} finally {
			try {
				statement.close() ;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
