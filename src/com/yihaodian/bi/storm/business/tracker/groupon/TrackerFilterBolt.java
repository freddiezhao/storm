package com.yihaodian.bi.storm.business.tracker.groupon;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.storm.business.tracker.parse.TrackerURLParser;
import com.yihaodian.bi.storm.common.model.TrackerVo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TrackerFilterBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7593191564622334469L;
	private static final Logger logger = LoggerFactory.getLogger(TrackerFilterBolt.class);
	private static final String GRPON_DETL_PC_PAGE_TYPE_ID = "123";
	private static final String GRPON_DETL_H5_PAGE_TYPE_ID = "5014";
	private static final String GRPON_DETL_APP_PAGE_TYPE_ID = "46200";
	private static final String GRPON_DETL_APP_PAGE_TYPE_ID2 = "46000";  // for compatibility
	
	private static final String MP_DETL_PC_PAGE_TYPE_ID = "295";//PC 295
	
	private static final String MP_DETL_H5_PAGE_TYPE_ID = "5022";//H5 5022,5104,5023
	private static final String MP_DETL_H5_PAGE_TYPE_ID1 = "5023";
	private static final String MP_DETL_H5_PAGE_TYPE_ID2 = "5104";
	
	private static final String MP_DETL_APP_PAGE_TYPE_ID = "319";//APP 319,94100
	private static final String MP_DETL_APP_PAGE_TYPE_ID1 = "94100";
	
	private static final String MP_DETL_APP_PAGE_TYPE_ID2 = "94000";     // for compatibility
	private static final String UNKNOWN = "-999999";
	
	private OutputCollector _collector;
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override @SuppressWarnings("unchecked")
	public void execute(Tuple input) {
		List<TrackerVo> trackerList = (ArrayList<TrackerVo>)input.getValue(0); 
		
		if (trackerList != null && !trackerList.isEmpty()) {
			for (TrackerVo t : trackerList) {
				if (t != null) {
					String url = t.getUrl();
					String pageTypeId = t.getPageTypeId();
				    String pageValue = t.getPageValue();
				    if (pageValue == null || pageValue.isEmpty())
				    	pageValue = UNKNOWN;
					if (GRPON_DETL_PC_PAGE_TYPE_ID.equals(pageTypeId)){
						_collector.emit("grpon_pc", input, new Values(t, pageValue));
					}
					else if (GRPON_DETL_H5_PAGE_TYPE_ID.equals(pageTypeId)){
						_collector.emit("grpon_h5", input, new Values(t, pageValue));
					}
					else if (GRPON_DETL_APP_PAGE_TYPE_ID.equals(pageTypeId) || GRPON_DETL_APP_PAGE_TYPE_ID2.equals(pageTypeId)){
						_collector.emit("grpon_app", input, new Values(t, pageValue));
					}
					else if (MP_DETL_PC_PAGE_TYPE_ID.equals(pageTypeId)) {
						_collector.emit("mp_pc", input, new Values(t));
					}
					else if (MP_DETL_H5_PAGE_TYPE_ID.equals(pageTypeId)|| MP_DETL_H5_PAGE_TYPE_ID1.equals(pageTypeId)|| MP_DETL_H5_PAGE_TYPE_ID2.equals(pageTypeId)) {
						_collector.emit("mp_h5", input, new Values(t));
					}
					else if (MP_DETL_APP_PAGE_TYPE_ID.equals(pageTypeId) ||MP_DETL_APP_PAGE_TYPE_ID1.equals(pageTypeId)||MP_DETL_APP_PAGE_TYPE_ID2.equals(pageTypeId)) {
						_collector.emit("mp_app", input, new Values(t));
					}
					else {
						try {
							TrackerURLParser parser = new TrackerURLParser(url);
							if (parser.isGrponDetl()) {
								_collector.emit("grpon_pc", input, new Values(t, parser.getGrponID()));
							}
							else if (parser.isGrponDetlH5()) {
								_collector.emit("grpon_h5", input, new Values(t, parser.getGrponID()));
							}
							else if (parser.isMpDetl()) {
								_collector.emit("mp_pc", input, new Values(t));
							}
							else if (parser.isMpDetlH5()) {
								_collector.emit("mp_h5", input, new Values(t));
							}
						} catch (MalformedURLException e) {
							logger.info("[INFO FROM HUJUN]: MalformedURLException caught!!! The URL:" + url);
	//							continue;
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
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("grpon_pc", new Fields("tracker", "grponId"));
		declarer.declareStream("grpon_h5", new Fields("tracker", "grponId"));
		declarer.declareStream("grpon_app", new Fields("tracker", "grponId"));
		declarer.declareStream("mp_pc", new Fields("tracker"));
		declarer.declareStream("mp_h5", new Fields("tracker"));
		declarer.declareStream("mp_app", new Fields("tracker"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
