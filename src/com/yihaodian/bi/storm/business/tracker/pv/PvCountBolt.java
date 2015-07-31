package com.yihaodian.bi.storm.business.tracker.pv;

import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.BaseDaoImpl;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;
import com.yihaodian.bi.storm.common.util.CommonUtil;
import com.yihaodian.bi.storm.common.util.Constant;
import com.yihaodian.bi.storm.common.util.Constant;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PvCountBolt implements IRichBolt {

	private static final Logger logger = LoggerFactory
			.getLogger(PvCountBolt.class);

	private BaseDaoImpl dao;

	OutputCollector _collector;
	int j = 0;

	private static long insertDB_startTime = System.currentTimeMillis();
	private static long insertDB_endTime = 0;

	/** 当天日期 */
	private String curTime = null;
	/** 线图-pv历史点入库rowkey */
	private String rowkey_pv_count = null;
	/** 线图-pv最后累加值入库rowkey */
	private String rowkey_pv_last = null;
	/** 坐标点信息 */
	private String[] strLastXValue = null;

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple input) {
		try {
			Long pv_count = input.getLong(0);
			strLastXValue = CommonUtil.getCurFullXValueStr();
			// 跨天操作
			String compd = DateUtil
					.transferDateToString(new Date(), "yyyyMMdd");
			// 非跨天才进行PV统计计算
			if (compd.equals(curTime)) {
				// 入库hbase,每50秒记录一次最后的更新值,用于页面加点
				// 入库hbase,每50秒记录一个历史点，用于页面刷新
				synchronized (this) {
					Long pv = pv_count * Constant.PV_EXECUTER_NUM;
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
				}

			} else {
				logger.info("===Pv count bole 跨天处理===" + compd);
				curTime = compd;
				rowkey_pv_last = "last_tracker_pv_" + curTime;
			}
			_collector.ack(input);
		} catch (Exception e) {
			_collector.fail(input);
			logger.info("===pv入库失败===" + e.getCause());
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		try {
			/** 初始化以下值 */
			dao = new BaseDaoImpl();
			this._collector = collector;
			curTime = DateUtil.transferDateToString(new Date(), "yyyyMMdd");
			rowkey_pv_last = "last_tracker_pv_" + curTime;
		} catch (Exception e) {
			logger.error("初始化Pv count bolt异常");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
