package com.yihaodian.bi.storm.logic.tracker.spout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.NotAliveException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.yihaodian.bi.cached.CMap;
import com.yihaodian.bi.cached.factory.CachedFactory;
import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.kafka.TrackerVO;
import com.yihaodian.bi.kafka.consumer.KafkaTrackerConsumerThread;
import com.yihaodian.bi.storm.TopoSubmitter;

/**
 * 类KafkaTrackerSpout.java的实现描述：TODO 类实现描述
 * 
 * @author zhaoheng Jul 20, 2015 1:48:51 PM
 */
public class KafkaTrackerSpout implements IRichSpout {

    public static final Logger           LOG                   = LoggerFactory.getLogger(KafkaTrackerSpout.class);
    /**
     * Tracker 批处理spout 类
     */
    private static final long            serialVersionUID      = -633828471355602266L;
    private KafkaTrackerConsumerThread   kafka                 = null;
    private SpoutOutputCollector         collector             = null;
    private String                       kafkaGroupId          = null;
    private CMap                         cache;
    private AtomicInteger                stat_emitCount        = new AtomicInteger(0);
    private AtomicInteger                stat_emitSuccessCount = new AtomicInteger(0);
    private AtomicInteger                stat_emitFailCount    = new AtomicInteger(0);
    private AtomicInteger                stat_emitMsg          = new AtomicInteger(0);
    private AtomicInteger                stat_emitSuccessMsg   = new AtomicInteger(0);
    private AtomicInteger                stat_emitFailMsg      = new AtomicInteger(0);
    private AtomicInteger                stat_tranSkip         = new AtomicInteger(0);
    private long                         lastPoint             = DateUtil.getLastCheckPointWholeFiveMinute();
    private Map<String, List<TrackerVO>> tranList              = new ConcurrentHashMap<String, List<TrackerVO>>();
    private int                          tranListMaxSize       = 2000;
    private boolean                      tran                  = true;


    public KafkaTrackerSpout(String kafkaGroupId){
        this.kafkaGroupId = kafkaGroupId;
    }

    @Override
    public void nextTuple() {
        writeStat();
        if (tran && tranList.size() > tranListMaxSize) {
            stat_tranSkip.incrementAndGet();
            return;
        }
        if (!kafka.getQueue().isEmpty()) {
            List<TrackerVO> list = new ArrayList<TrackerVO>(100);
            TrackerVO tk;
            while (true) {
                tk = kafka.getQueue().poll();
                if (tk != null) {
                    list.add(tk);
                    if (list.size() >= 100) {
                        break;
                    }
                } else {
                    break;
                }
            }
            if (list.size() > 0) {
                stat_emitCount.addAndGet(list.size());
                stat_emitMsg.incrementAndGet();
                if (tran) {
                    tranList.put(list.get(0).getId(), list);
                }
                collector.emit(new Values(list), list.get(0).getId());
            }
        }
    }

    private void writeStat() {
        try {
            long cur = System.currentTimeMillis();
            if (cur - lastPoint > DateUtil.MINUTE_FIVE) {
                String dt = DateUtil.YYYYMMDDHHMM.format(new Date(cur));
                System.out.println(dt + " " + toString());
                int emitCount = stat_emitCount.get();
                int emitFailCount = stat_emitFailCount.get();
                int emitSuccessCount = stat_emitSuccessCount.get();
                int emitSuccMsg = stat_emitSuccessMsg.get();
                int emitFailMsg = stat_emitFailMsg.get();
                int emitMsg = stat_emitMsg.get();
                int tranSkip = stat_tranSkip.get();

                stat_emitCount.set(0);
                stat_emitFailCount.set(0);
                stat_emitSuccessCount.set(0);
                stat_emitMsg.set(0);
                stat_emitFailMsg.set(0);
                stat_emitSuccessMsg.set(0);
                stat_tranSkip.set(0);

                cache.incr("tracker_stat_spout_emit_" + dt, emitCount);
                cache.incr("tracker_stat_spout_emit_succ_" + dt, emitSuccessCount);
                cache.incr("tracker_stat_spout_emit_fail_" + dt, emitFailCount);
                cache.incr("tracker_stat_spout_msg_" + dt, emitMsg);
                cache.incr("tracker_stat_spout_msg_succ_" + dt, emitSuccMsg);
                cache.incr("tracker_stat_spout_msg_fail_" + dt, emitFailMsg);
                cache.incr("tracker_stat_spout_tran_skip_" + dt, tranSkip);

                lastPoint = cur;
            }
        } catch (Exception e1) {
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            init();
            kafka.setIsRun(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void init() throws IOException {
        kafka = new KafkaTrackerConsumerThread(kafkaGroupId, false);
        kafka.start();
        cache = CachedFactory.getDefaultHBaseAsyncLRU();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("track"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void ack(Object msgId) {
        stat_emitSuccessMsg.incrementAndGet();
        if (tran) {
            List list = tranList.get(msgId.toString());
            if (list != null) {
                stat_emitSuccessCount.addAndGet(tranList.get(msgId.toString()).size());
                tranList.remove(msgId.toString());
            }
        }
    }

    @Override
    public void activate() {

    }

    @Override
    public void close() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void fail(Object msgId) {
        stat_emitFailMsg.incrementAndGet();
        if (tran) {
            List<TrackerVO> rlist = tranList.get(msgId.toString());
            if (rlist != null) {
                stat_emitFailCount.addAndGet(rlist.size());
                collector.emit(new Values(rlist), rlist.get(0).getId());
            }
        }
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, NotAliveException,
                                          TException, InterruptedException {
        TopoSubmitter.submit(new String[] { "debug", "BI_Needle" });
        while (true) {
            Thread.sleep(10000);
        }
    }
}
