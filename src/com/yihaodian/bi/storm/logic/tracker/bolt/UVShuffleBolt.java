package com.yihaodian.bi.storm.logic.tracker.bolt;

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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.yihaodian.bi.cached.CMap;
import com.yihaodian.bi.cached.factory.CachedFactory;
import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.kafka.TrackerVO;

public class UVShuffleBolt implements IRichBolt {

    private static final long   serialVersionUID = -5672758362296831352L;
    private static final Logger LOG              = LoggerFactory.getLogger(UVShuffleBolt.class);

    private OutputCollector     _collector;
    private String              guid;
    private int                 stat_guidNotNull = 0;
    private int                 stat_guidNull    = 0;
    private CMap                cache;
    private long                lastPoint        = DateUtil.getLastCheckPointWholeFiveMinute();

    @Override
    public void execute(Tuple input) {
        try {
            List<TrackerVO> list = (List<TrackerVO>) input.getValueByField("track");
            for (TrackerVO t : list) {
                guid = t.getGu_id();
                if (guid != null) {
                    stat_guidNotNull++;
                    _collector.emit(new Values(guid, t));
                } else {
                    stat_guidNull++;
                }
            }
            writeStat();
            _collector.ack(input);
        } catch (Exception e) {
            LOG.error("Shuffling trackers Failed! ", e);
            _collector.fail(input);
        }
    }

    private void writeStat() throws Exception {
        long cur = System.currentTimeMillis();
        if (cur - lastPoint > DateUtil.MINUTE_FIVE) {
            String dt = DateUtil.YYYYMMDDHHMM.format(new Date(cur));
            cache.incr("tracker_stat_bolt_guid_null_" + dt, stat_guidNull);
            cache.incr("tracker_stat_bolt_guid_notnull_" + dt, stat_guidNotNull);
            stat_guidNull = 0;
            stat_guidNotNull = 0;
            lastPoint = cur;
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        try {
            cache = CachedFactory.getDefaultHBaseAsyncLRU();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("guid", "track"));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
