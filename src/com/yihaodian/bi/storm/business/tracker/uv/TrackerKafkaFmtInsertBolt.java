package com.yihaodian.bi.storm.business.tracker.uv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.hbase.dao.TrackerDao;
import com.yihaodian.bi.hbase.dao.impl.TrackerDaoImpl;
import com.yihaodian.bi.storm.common.model.TrackerVo;
import com.yihaodian.bi.storm.common.util.Constant;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TrackerKafkaFmtInsertBolt implements IBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory
	.getLogger(TrackerKafkaFmtInsertBolt.class);
	

	List<String> rowkeyList = null;
	String rowkey = null;
	TrackerDao dao;
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// 
		String track = input.getString(0);
		if (track != null) {
			String arr[] = track.split("\n") ;
			List<TrackerVo> list = new ArrayList<TrackerVo>();
			for (String s : arr) 
			{
				String cols[] = s.split("\\t");
//				System.out.println("cols:"+cols.length);
				// 过滤一：列小于30的丢弃
				if (cols.length < 30) {
					continue ;
				}
				// 过滤二：过滤来自百度的爬虫数据
				if (s.contains("http://www.baidu.com/search/spider.html")) {
					continue ;
				}
				TrackerVo tracker = new TrackerVo(s);
				// 过滤三：过滤button_position 不为空的
				if (tracker.getButton_position() != null 
						&& !"null".equals(tracker.getButton_position()) && tracker.getButton_position().length()>1) {
//					System.out.println("button:"+tracker.getButton_position());
					continue ;
				}
				list.add(tracker);
//				System.out.println(arr.length+"------:"+cols.length+"--"+tracker.getId());
			}
			if (list.size()>0) {
				// insert db
				rowkeyList = new ArrayList<String>();
				System.err.println("list.size=================:"+list.size());
				System.err.println(Thread.currentThread().getName()+" TrackUvInsertBolt  list.size===trackTime==============:"+list.get(0).getTrack_time());
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
				collector.emit(new Values(rowkey));
			}
			
			
		}
		
		
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		try {
			dao = new TrackerDaoImpl();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("rowkey"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
