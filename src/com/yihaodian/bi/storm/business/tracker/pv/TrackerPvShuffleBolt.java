package com.yihaodian.bi.storm.business.tracker.pv;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.hbase.dao.impl.BaseDaoImpl;
import com.yihaodian.bi.storm.common.util.Constant;

public class TrackerPvShuffleBolt implements IRichBolt {

	private static final long serialVersionUID = -6068004177883552012L;

	private static final Logger logger = LoggerFactory
			.getLogger(TrackerPvShuffleBolt.class);

	/** 当天日期 */
	private String curTime = DateUtil.transferDateToString(new Date(),
			"yyyyMMdd");

	int i = 0;

	private long pv_count = 0;
	private BaseDaoImpl dao;

	OutputCollector _collector;
	String rowkey_pv_last;

	@Override
	public void execute(Tuple tuple) {
		try {
			// 跨天操作
			String compd = DateUtil
					.transferDateToString(new Date(), "yyyyMMdd");
			// 非跨天才进行PV累加
			if (compd.equals(curTime)) {
				pv_count++;
				_collector.emit(new Values(pv_count));
			} else {
				logger.info("===tracker count bolt pv 跨天处理===" + compd);
				curTime = compd;
				pv_count = 0;
			}
			_collector.ack(tuple);
		} catch (Exception e) {
			_collector.fail(tuple);
			logger.info("===tracker count bolt pv 处理失败===" + e.getCause());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("addpv"));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this._collector = collector;
		try {
			dao = new BaseDaoImpl();
		} catch (IOException e) {
			logger.error("Hbase Connetion not established. Caused by:", e);
		}
		rowkey_pv_last = "last_tracker_pv_" + curTime;
		try {
			pv_count = Long.parseLong(dao.getColumnValue(Constant.TABLE_TRACKER_PV_RESULT, 
					rowkey_pv_last, Constant.COMMON_FAMILY, "tracker_pv"))/Constant.PV_EXECUTER_NUM;
		} catch (NumberFormatException e) {
			logger.error("Initial pv_count error. Caused by: ", e);
		} catch (IOException e) {
			logger.error("Retrieving " + rowkey_pv_last + "from HBase failed. Caused by:", e);
		}
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
