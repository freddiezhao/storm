package com.yihaodian.bi.storm.business.tracker;

import java.util.List;
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

import com.yihaodian.bi.storm.common.model.TrackerVo;

public class TrackerRecountBolt implements IRichBolt {

	private static final long serialVersionUID = 1362499877042085829L;

	private static final Logger logger = LoggerFactory
			.getLogger(TrackerRecountBolt.class);
	
	OutputCollector _collector;

	@Override
	public void execute(Tuple tuple) {
		try {
			List<TrackerVo> list = (List<TrackerVo>) tuple.getValueByField("track");
			logger.info("tracker list size is " + list.size());
			String guid = null;
			for (TrackerVo t : list) {
				guid = t.getGu_id();
				if (guid == null || guid.length() < 5) {
					continue;
				}
				_collector.emit(new Values(guid));
			}
			_collector.ack(tuple);
		} catch (Exception e) {
			_collector.fail(tuple);
			logger.info("===tracker list 处理失败===" + e.getCause());
		}
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("guid"));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
