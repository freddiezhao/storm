package com.yihaodian.bi.storm.business.tracker.pv;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.hbase.dao.impl.BaseDaoImpl;
import com.yihaodian.bi.storm.common.model.TrackerVo;
import com.yihaodian.bi.storm.common.util.Constant;

public class PVIncBolt  implements IRichBolt{

	private static final long serialVersionUID = 3913358775431868228L;
	private static Logger LOG = LoggerFactory.getLogger(PVIncBolt.class);
	private BaseDaoImpl dao;
	private String[] strLastXValue;
	private String curTime;
	long insertDB_endTime;
	long insertDB_startTime;
	String rowkey_pv_count;
	String rowkey_pv_last;
	private OutputCollector _collector;
	Map<String, Long> pv;
	static HTable hbaseTable;
	static {
		try {
	        hbaseTable = new HTable(HBaseConfiguration.create(), Constant.TABLE_TRACKER_PV_TEST);
		} catch (IOException e) {
			LOG.info("HBase Initializing Failed.");
		}
	}
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		_collector = collector;
		try {
			dao = new BaseDaoImpl();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
			@SuppressWarnings("unchecked")
			List<TrackerVo> trackerList = (List<TrackerVo>)input.getValue(0);
			
			// 按时间窗口累加PV
			for (TrackerVo t : trackerList) {
				String bucket = getBucket(t);
				try {
					hbaseTable.incrementColumnValue(bucket.getBytes(), Constant.COMMON_FAMILY.getBytes(),
								"tracker_pv".getBytes(), 1L);
				} catch (IOException e) {
					LOG.info("Increasing Failed!", e);
				}
			}
	}

	private String getBucket(TrackerVo t) {
		String tt = t.getTrack_time();
		if (tt == null || tt.isEmpty()) {
			tt = curTime = DateUtil.getFmtDate(new Date(), DateUtil.YYYY_MM_DD_HH_MM_SS_STR);
		}
		String str = tt.replaceAll("[-: ]", "");
		
		return str.substring(0, str.length()-2);
	}

	@Override
	public void cleanup() {
		try {
			hbaseTable.close();
		} catch (IOException e) {
			LOG.info("HBase closing Failed.");
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
