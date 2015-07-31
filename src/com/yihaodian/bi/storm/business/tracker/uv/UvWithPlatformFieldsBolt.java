package com.yihaodian.bi.storm.business.tracker.uv;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.yihaodian.bi.common.util.DateUtil;

/**
 * @author zhangsheng2
 * uv分平台 分组处理uv app h5
 */
public class UvWithPlatformFieldsBolt extends BaseBasicBolt {

	private static final Logger logger = LoggerFactory
			.getLogger(UvWithPlatformFieldsBolt.class);

	Map<String, Integer> counts = new HashMap<String, Integer>();

	/** 当天日期 */
	private String curTime = DateUtil.transferDateToString(new Date(),
			"yyyyMMdd");

	int i = 0;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		try {
			String guid = tuple.getString(0);
			String url = tuple.getString(1);
			String container = tuple.getString(2);
			String platform = tuple.getString(3);
			String type = confirmType(url,container,platform);
			// 跨天操作
			String compd = DateUtil
					.transferDateToString(new Date(), "yyyyMMdd");
			// 非跨天才进行UV统计计算
			if (compd.equals(curTime)) {
				Integer count = null;
				count = counts.get(guid);
				// 如果count为空,则表示该线程处理的guid分组多了一个新的,算作一个UV,则发送消息给下一级bolt,通知下一级bolt的uv计数器+1
				if (count == null) {
					i++;
					count = 0;
					collector.emit(new Values(1,type));
				}
				// 这里的count数暂时没有用到,之前的用处在于统计之后传递到下一级bolt遍历map求pv的值.
				count++;
				counts.put(guid, count);
			} else {
				logger.info("===tracker count bolt 跨天处理===" + compd);
				curTime = compd;
				counts = new HashMap<String, Integer>();
			}
		} catch (Exception e) {
			logger.info("===tracker count bolt 处理失败===" + e.getCause());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//type 0 app 1 h5
		declarer.declare(new Fields("adduv","type"));
	}
	
	/**
	 * TODO 确定类型 0 app 1 h5
	 * @param url
	 * @param container
	 * @return 
	 */
	private String confirmType(String url,String container,String platform){
		String type = "0";
		if((container!=null&&container.toLowerCase().equals("yhdapp"))){
			type = "0";
		}else if((container!=null&&!container.toLowerCase().equals("yhdapp"))&&(platform.toLowerCase().equals("iossystem")||platform.toLowerCase().equals("ipadsystem")||platform.toLowerCase().equals("androidsystem"))){
			type = "1";
		}else if((platform!=null&&(platform.toLowerCase().equals("iossystem")||platform.toLowerCase().equals("ipadsystem")||platform.toLowerCase().equals("androidsystem")))){
			type = "0";
		}else if((url.indexOf("http://m.yihaodian.com") != -1 ||url.indexOf("http://m.yhd.com") != -1 || url.indexOf("http://m.1mall.com") != -1)){
			type = "1";
		}else{
			type = "-1";
		}
		return type;
	}
}
