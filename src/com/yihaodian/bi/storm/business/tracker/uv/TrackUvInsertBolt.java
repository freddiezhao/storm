package com.yihaodian.bi.storm.business.tracker.uv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
import com.yihaodian.bi.hbase.dao.TrackerDao;
import com.yihaodian.bi.hbase.dao.impl.TrackerDaoImpl;
import com.yihaodian.bi.storm.common.model.TrackerVo;
import com.yihaodian.bi.storm.common.util.Constant;
/**
 * 
 * 4.2.1 UV数，按天
 * bi_monitor_uv_tmp 基础表 rowkey设计：reverse(201405201120)_guid
 * 
 * bi_monitor_uv_rslt 结果表rowkey设计：201405201120
 * 
 * 该类线程安全，可开多线程
 * 
 * by lvpeng
 */ 
public class TrackUvInsertBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;

	OutputCollector _collector;

	private static final Logger logger = LoggerFactory
			.getLogger(TrackUvInsertBolt.class);

	TrackerDao dao;

	private String rowkey = null;

	public TrackUvInsertBolt() {
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple input) {
//		rowkey = StringUtils.reverse(DateFmat.getCurMinute());
		logger.info("-----TrackUvInsertBolt---execute-----");
		List<TrackerVo> list = (List<TrackerVo>)input.getValue(0);
		if (list.size() > 0) {
			List<String> rowkeyList = new ArrayList<String>();
			System.err.println("list.size=================:"+list.size());
			System.err.println(Thread.currentThread().getName()+" TrackUvInsertBolt  list.size=================:"+list.get(0).getUrl());
			for(TrackerVo tracker : list)
			{
				if (tracker.getGu_id()==null || tracker.getGu_id().length()<5) {
					continue ;
				}
				rowkey = StringUtils.reverse(DateUtil.getCountDate(tracker.getTrack_time(), DateUtil.YYYYMMDDHHMM_STR));
				rowkeyList.add(rowkey+"_"+tracker.getGu_id());
			}
			try {
				dao.insertRowkeys(Constant.TABLE_UV_TMP, rowkeyList, Constant.COMMON_FAMILY, "guid", false, false, 2 * 1024 * 1024) ;// 2M
			} catch (IOException e) {
				logger.info(Constant.TABLE_UV_TMP+" put  faild ! pls  check !") ;
				e.printStackTrace();
			} 
			_collector.emit(new Values("exec_flag"));
		}
		_collector.ack(input);

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
//		System.out.println("start prepare---");
		try {
			dao = new TrackerDaoImpl();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("exec_flag"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
