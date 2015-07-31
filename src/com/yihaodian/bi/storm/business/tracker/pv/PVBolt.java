package com.yihaodian.bi.storm.business.tracker.pv;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.hbase.dao.BaseDao;
import com.yihaodian.bi.hbase.dao.impl.BaseDaoImpl;
import com.yihaodian.bi.storm.common.model.TrackerVo;
import com.yihaodian.bi.storm.common.util.CommonUtil;
import com.yihaodian.bi.storm.common.util.Constant;

public class PVBolt  implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7071675330164036801L;
	private BaseDao dao;
	private String[] strLastXValue;
	private String curTime;
	long insertDB_endTime;
	long insertDB_startTime;
	String rowkey_pv_count;
	String rowkey_pv_last;
	private OutputCollector _collector;
	long pv;
	
	private static Logger logger = LoggerFactory.getLogger(PVBolt.class);
	
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
		curTime = DateUtil.getFmtDate(new Date(), DateUtil.YYYYMMDD_STR);
		rowkey_pv_last = "last_tracker_pv_" + curTime;
		
		try {
			String tmp = dao.getColumnValue(Constant.TABLE_TRACKER_PV_RESULT, 
					rowkey_pv_last, Constant.COMMON_FAMILY,"tracker_pv" );
			if (tmp.isEmpty())
				pv = 0;
			else
				pv = Long.parseLong(tmp);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void execute(Tuple input) {
		try {
			@SuppressWarnings("unchecked")
			List<TrackerVo> trackerList = (List<TrackerVo>)input.getValue(0);
			pv += trackerList.size();
			strLastXValue = CommonUtil.getCurFullXValueStr();
			// 跨天操作
			String compd = DateUtil.getFmtDate(new Date(), DateUtil.YYYYMMDD_STR);
			// 非跨天才进行PV统计计算
			if (compd.equals(curTime)) {
				// 入库hbase,每50秒记录一次最后的更新值,用于页面加点
				// 入库hbase,每50秒记录一个历史点，用于页面刷新
					insertDB_endTime = System.currentTimeMillis();
					if (insertDB_endTime - insertDB_startTime >= Constant.SECOND_50) {
						/**
						 * 线图数据处理
						 */
						rowkey_pv_count = curTime + "_pv_" + strLastXValue[0].replace(".", "");
						// 插入点的历史数据,线图中用来拼接历史数据线条
						dao.insertRecord(
										Constant.TABLE_TRACKER_PV_RESULT,
										rowkey_pv_count,
										Constant.COMMON_FAMILY,
										new String[] { "tracker_pv", "xValue",
												"xTitle" }, new String[] {
												pv + "", strLastXValue[1],
												strLastXValue[0] });

						// 用于页面加点
						dao.insertRecord(
										Constant.TABLE_TRACKER_PV_RESULT,
										rowkey_pv_last,
										Constant.COMMON_FAMILY,
										new String[] { "tracker_pv", "xValue",
												"xTitle" }, new String[] {
												pv + "", strLastXValue[1],
												strLastXValue[0] });

						logger.info("===pv入库成功===");
						insertDB_startTime = System.currentTimeMillis();
					}

			} else {
				logger.info("===Pv count bole 跨天处理===" + compd);
				curTime = compd;
				rowkey_pv_last = "last_tracker_pv_" + curTime;
				pv = 0;
			}
			_collector.ack(input);
		} catch (Exception e) {
			_collector.fail(input);
			logger.info("===pv入库失败===" + e.getCause());
		}
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
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
