package com.yihaodian.bi.storm.business.tracker.groupon;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.hbase.dao.TrackerDao;
import com.yihaodian.bi.hbase.dao.impl.TrackerDaoImpl;
import com.yihaodian.bi.storm.common.model.TrackerVo;
import com.yihaodian.bi.storm.common.util.Constant;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TrackMpUidBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2685720133110687744L;
	private static final Logger logger= LoggerFactory.getLogger(TrackMpUidBolt.class);
	private OutputCollector _collector;
	private TrackerDao _dao;
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		TrackerVo tracker = (TrackerVo)input.getValue(0);
		
		if (tracker != null) { 
			String dateID = DateUtil.getCountDate(tracker.getTrack_time(), DateUtil.YYYYMMDD_STR);
			String uid = tracker.getEnd_user_id();
			String prodID = tracker.getProduct_id();
			String url = tracker.getUrl();
			
			if (dateID != null && !dateID.isEmpty() && 
					prodID != null && !prodID.isEmpty()) {
				if (uid != null && !uid.isEmpty()) {
					logger.info("[INFO FROM HUJUN]: " + uid);
					String rowKey = dateID + "_" +  uid + "_" + prodID;
					try {
						_dao.insertRecord(Constant.TABLE_MP_USER, rowKey, Constant.COMMON_FAMILY, new String[]{"url","flag"}, new String[]{url,"0"});
					} catch(IOException e) {
						logger.error("Failed to insert to Hbase!!!");
					}
				}
				else {
					String guid = tracker.getGu_id();
					if (guid != null && !guid.isEmpty()) {
						try {
							Result res = _dao.getOneRecord(Constant.TABLE_GUID_UID, guid);
							if (!res.isEmpty()) {
								byte[] uidBytes = res.getValue((Constant.COMMON_FAMILY).getBytes(), "end_user_id".getBytes());
								if (uidBytes != null) {
									uid = new String(uidBytes);
									if (!uid.isEmpty()) {
										logger.info("[INFO FROM HUJUN]: retrieving uid:: " + uid);
										String rowKey = dateID + "_" + uid + "_" + prodID;
										_dao.insertRecord(Constant.TABLE_MP_USER, rowKey, Constant.COMMON_FAMILY, new String[]{"url","flag"}, new String[]{url,"1"});
									}
								}
							}
						} catch (IOException e) {
							logger.error("Failed to operate Hbase!!!");
						}
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
}
