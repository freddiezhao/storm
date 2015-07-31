package com.yihaodian.bi.storm.business.tracker.groupon;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.database.DBConnection;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.TrackerDao;
import com.yihaodian.bi.hbase.dao.impl.TrackerDaoImpl;
import com.yihaodian.bi.storm.common.model.TrackerVo;
import com.yihaodian.bi.storm.common.util.Constant;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TrackGrponDetlBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6000311276118384018L;
	private static final Logger logger= LoggerFactory.getLogger(TrackGrponDetlBolt.class);
	private OutputCollector _collector;
	private DBConnection _dbConnection;
	private Connection _connection;
	private TrackerDao _dao;
	
	private static final Integer grponFlag = 5;
	private static final String SCHEMA = "rpt";
	private static final Integer GRPON_PC = 101;
	private static final Integer GRPON_H5 = 102;
	private static final Integer GRPON_APP = 103;
	private static final Integer UNKNOWN = -999999;
	private static final String  NULL="";
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		String  streamId = input.getSourceStreamId();
		TrackerVo tracker = (TrackerVo)input.getValueByField("tracker");
		
		if (tracker != null) { 
			String dateTimeId = DateUtil.getCountDate(tracker.getTrack_time(), DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
			String uid = tracker.getEnd_user_id();
			String sid = tracker.getSession_id();
			String guid= tracker.getGu_id();
			String prodId = tracker.getProduct_id();
			String pmId = tracker.getExt_field7(); // pm_info_id
			Integer trackSource = UNKNOWN;
			if (streamId.equals("grpon_pc"))
				trackSource = GRPON_PC;
			else if (streamId.equals("grpon_h5"))
				trackSource = GRPON_H5;
			else if (streamId.equals("grpon_app"))
				trackSource = GRPON_APP;
			
			String grouponID = input.getStringByField("grponId");
			try {
				Result res = _dao.getOneRecord(Constant.TABLE_DIM_GROUPON, grouponID);
				if (!res.isEmpty()) {
					byte[] prodIDBytes = res.getValue((Constant.COMMON_FAMILY).getBytes(), "prod_id".getBytes());
					if (prodIDBytes != null) {
						 prodId = new String(prodIDBytes);
					}
				}
			} catch(IOException e) {
				logger.error("Failed to query to Hbase!!!");
			}
			
			
//			logger.info("Dateid: " + dateTimeId + "|" +
//		    "uid   : " + uid    + "|" +
//		    "sid   : " + sid    + "|" +
//		    "guid  : " + guid   + "|" + 
//		    "prodID: " + prodId + "|" +
//		    "pmId  : " + pmId   + "|" +
//		    "trackSoure" + trackSource );
//			if (dateTimeId != null && !dateTimeId.isEmpty() &&
//					prodId != null && !prodId.isEmpty() || pmId != null && !pmId.isEmpty()) {
				String update_time = DateUtil.getFmtDate(null, DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
				insertToOracle(_connection, dateTimeId, uid, sid, guid, prodId, pmId, grponFlag, trackSource, update_time);
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
		try {
			_dao = new TrackerDaoImpl();
		} catch (Exception e) {
			logger.error("Failed to create dao!");
			e.printStackTrace();
		}
		
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
	private static void insertToOracle(Connection con, String date_time_id, String uid, String sid, String guid, String prodId, String pmId, int flag, Integer trackSource, String update_time){
		Statement statement = null;
		
		try {
			statement = con.createStatement();
			
//			if (uid == null || uid.isEmpty()) uid = UNKNOWN.toString();
			
			String sql = 
				"insert into " + SCHEMA + "." + Constant.TABLE_TRACK_GROUPON_MP_DETL + "(DATE_ID, DATE_TIME_ID, END_USER_ID, SESSION_ID, GUID, PROD_ID, PM_ID, PAGE_TYPE_ID, TRACK_SOURCE, UPDATE_TIME) " +
		  		"values (trunc(to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss')), to_date('"+date_time_id+"','yyyy-MM-dd hh24:mi:ss'), '" +  uid + "', '" + sid + "', '" + guid + "', '" + prodId + "', '" + pmId + "', '" + flag + "', '" + trackSource + "', " + "to_date('" + update_time + "','yyyy-MM-dd hh24:mi:ss'))";
			statement.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.info("Insert Oracle failed. Dateid: " + date_time_id + "uid   : " + uid    + "sid   : " + sid    + "guid  : " + guid   + "prodID: " + prodId + "pmId  : " + pmId   + "trackSoure" + trackSource);
		} finally {
			try {
				statement.close() ;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
