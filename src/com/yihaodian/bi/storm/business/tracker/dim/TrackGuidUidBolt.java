package com.yihaodian.bi.storm.business.tracker.dim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.hbase.dao.TrackerDao;
import com.yihaodian.bi.hbase.dao.impl.TrackerDaoImpl;
import com.yihaodian.bi.storm.common.model.TrackerVo;
import com.yihaodian.bi.storm.common.util.Constant;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * 
 * @author hujun1
 *
 */
public class TrackGuidUidBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3064360359618894308L;
	private static final Logger logger = LoggerFactory.getLogger(TrackGuidUidBolt.class);
	private static long  prevTime = System.currentTimeMillis();
	private static long  currTime = 0;
	private Map<String, String> _guidUidMap = new HashMap<String, String>(); 
	private OutputCollector _collector;
	private TrackerDao _dao;

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	
	
	@Override  @SuppressWarnings("unchecked")
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		List<TrackerVo> trackerList = (ArrayList<TrackerVo>)input.getValue(0);
		if (trackerList != null && !trackerList.isEmpty()) {
			for (TrackerVo t : trackerList) {
				if (t != null) {
					String guid = t.getGu_id();
					String uid  = t.getEnd_user_id();
					if (guid != null && !guid.isEmpty() && !guid.equals("null") && uid != null && !uid.isEmpty() && !uid.equals("null") ) {
						_guidUidMap.put(guid, uid);
					}
				}
			}
		}	
		_collector.ack(input);
		
		//put data into Hbase every 5 minutes
		currTime = System.currentTimeMillis();
		if (currTime - prevTime >= 60000) {
			Iterator it = _guidUidMap.keySet().iterator();
			while (it.hasNext()) {
				String guid = (String)it.next();
				String uid  = _guidUidMap.get(guid);
				try {
					_dao.insertRecord(Constant.TABLE_GUID_UID, guid, Constant.COMMON_FAMILY, "end_user_id", uid);
				} catch (IOException e) {
					logger.error("Failed to insert to Hbase!");
					e.printStackTrace();
				}
			}
			_guidUidMap.clear();
			prevTime = System.currentTimeMillis();
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector =collector;
		try {
			_dao = new TrackerDaoImpl();
		} catch (Exception e) {
			logger.error("Failed to create dao!");
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
