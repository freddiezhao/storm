package com.yihaodian.bi.storm.business.tracker.uv;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.yihaodian.bi.storm.common.model.TrackerVo;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TrackerKafkaFmtBolt implements IBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

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
				collector.emit(new Values(list));
			}
		}
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("trackList"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
