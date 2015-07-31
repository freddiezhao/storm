package com.yihaodian.bi.storm.business.tracker.uv;

import java.util.Date;
import java.util.HashMap;
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

public class UvCountBolt implements IRichBolt {

	private static final Logger logger = LoggerFactory
			.getLogger(UvCountBolt.class);

	private BaseDaoImpl dao;

	OutputCollector _collector;
	int j = 0;

	private long uv_count;

	private static long insertDB_startTime = System.currentTimeMillis();
	private static long insertDB_endTime = 0;

	/** 当天日期 */
	private String curTime = null;
	/** 线图-uv历史点入库rowkey */
	private String rowkey_uv_count = null;
	/** 线图-uv最后累加值入库rowkey */
	private String rowkey_uv_last = null;
	/** 坐标点信息 */
	private String[] strLastXValue = null;

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple input) {
		try {
			strLastXValue = CommonUtil.getCurFullXValueStr();
			// 跨天操作
			String compd = DateUtil
					.transferDateToString(new Date(), "yyyyMMdd");
			// 非跨天才进行UV统计计算
			if (compd.equals(curTime)) {
				// uv计数器
				uv_count++;
				System.out.println("接收uv次数："+uv_count);
				// 入库hbase,每50秒记录一次最后的更新值,用于页面加点
				// 入库hbase,每50秒记录一个历史点，用于页面刷新
				synchronized (this) {
					insertDB_endTime = System.currentTimeMillis();
					if (insertDB_endTime - insertDB_startTime >= Constant.SECOND_50) {
						/**
						 * 线图数据处理
						 */
						rowkey_uv_count = curTime + "_uv_" + strLastXValue[0].replace(".", "");
						// 插入点的历史数据,线图中用来拼接历史数据线条
						dao.insertRecord(
										Constant.TABLE_TRACKER_UV_RESULT,
										rowkey_uv_count,
										Constant.COMMON_FAMILY,
										new String[] { "tracker_uv", "xValue",
												"xTitle" }, new String[] {
												uv_count + "",
												strLastXValue[1],
												strLastXValue[0] });

						// 用于页面加点
						dao.insertRecord(
										Constant.TABLE_TRACKER_UV_RESULT,
										rowkey_uv_last,
										Constant.COMMON_FAMILY,
										new String[] { "tracker_uv", "xValue",
												"xTitle" }, new String[] {
												uv_count + "",
												strLastXValue[1],
												strLastXValue[0] });

						logger.info("===uv入库成功===");
						insertDB_startTime = System.currentTimeMillis();
					}
				}

			} else {
				logger.info("===Uv count bole 跨天处理===" + compd);
				curTime = compd;
				rowkey_uv_last = "last_tracker_uv_" + curTime;
				uv_count = 0;
			}
			_collector.ack(input);
		} catch (Exception e) {
			_collector.fail(input);
			logger.info("===uv入库失败===" + e.getCause());
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
			rowkey_uv_last = "last_tracker_uv_" + curTime;
			uv_count = Long.parseLong(dao.getColumnValue(Constant.TABLE_TRACKER_UV_RESULT, 
					rowkey_uv_last, Constant.COMMON_FAMILY, "tracker_uv"));
		} catch (Exception e) {
			logger.error("初始化Uv count bolt异常");
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
