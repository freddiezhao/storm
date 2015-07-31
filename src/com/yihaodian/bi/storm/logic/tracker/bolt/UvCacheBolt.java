package com.yihaodian.bi.storm.logic.tracker.bolt;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.yihaodian.bi.cached.CMap;
import com.yihaodian.bi.cached.factory.CachedFactory;
import com.yihaodian.bi.kafka.TrackerVO;
import com.yihaodian.bi.storm.common.util.Constant;
import com.yihaodian.bi.storm.logic.tracker.common.Utils;

public class UvCacheBolt implements IRichBolt {

    private static final long   serialVersionUID = 4195223561461461950L;

    private static final Logger LOG              = LoggerFactory.getLogger(UvCacheBolt.class);

    OutputCollector             _collector;

    private CMap                cache;
    private CMap                auxCache;

    private String[]            preStatus        = new String[3];                             // pltmId , provId,
                                                                                               // chnlId
    private String[]            curStatus        = new String[3];
    private TrackerVO           t;
    private String              win;
    private String              globWin;
    private String              guid;
    private String              statusKey;
    private String              globStatusKey;
    private String              globStatKey;

    @Override
    public void cleanup() {

    }

    @Override
    public void execute(Tuple input) {
        try {
            guid = input.getStringByField("guid");
            t = (TrackerVO) input.getValueByField("track");
            String timeStamp = t.getTrack_time().replaceAll("[-: ]", "");
            globWin = timeStamp.substring(0, 8); // 全局窗口（20150724）
            win = timeStamp.substring(0, 11) + "0"; // win为时间窗口（10分钟）

            statusKey = globWin + "_" + guid + "_" + t.getSession_id(); // 判断有效UV用
            globStatusKey = globWin + "_" + guid; // 去重用
            globStatKey = "glob_" + win; // 全局统计
            if (cache.get(statusKey) == null) { // 非有效
                String pltm = Utils.pltmType(t);
                if (pltm == "4" || pltm == "-1") {
                    updateCacheH5PC("H5PC_" + guid, t.getEnd_user_id(), pltm);
                }
                cache.put(statusKey,
                          StringUtils.join(new String[] { pltm, t.getExt_field2(), Utils.channel(auxCache, t) }, "_")); // 更新statusCache

            } else { // 有效（但需要去重）
                setPreState(cache.get(statusKey));
                setCurState(new String[] { Utils.pltmType(t), t.getExt_field2(), Utils.channel(auxCache, t) });
                updateCache();
            }
            _collector.ack(input);
            
        } catch (Exception e) {
            LOG.error("UvCacheBolt exception " + input.getMessageId(), e);
            _collector.fail(input);
        }
    }

    private void updateCacheH5PC(String key, String usr_id, String pltm) throws Exception {
        String gKey = "H5PC_" + guid + "_" + globWin; // H5，PC 的全局key 是 H5PC_guid_20150724
        String uid = cache.get(gKey);

        String prefix0 = "user_" + pltm + "_0_"; // 非会员
        String prefix1 = "user_" + pltm + "_1_"; // 会员

        if (uid == null) {
            cache.put(gKey, usr_id);
            if (usr_id.isEmpty()) {
                cache.incr(prefix0 + win, 1);
            } else {
                cache.incr(prefix1 + win, 1);
            }
        } else {
            if (uid.isEmpty() && !usr_id.isEmpty()) {
                cache.put(gKey, usr_id);
                cache.incr(prefix1 + win, 1);
                cache.incr(prefix0 + win, -1);
            }
        }
    }

    private void updateCache() throws Exception {

        String[] update = new String[] { "", "", "" };
        boolean flag = false;

        if (cache.get(globStatusKey) == null) { // 全局不存在该guid
            flag = true;

            // 全站
            cache.put(globStatusKey, "1");
            cache.incr(globStatKey, 1);
//            System.out.println(globStatusKey);

            // app有效UV
            if (curStatus[0] == "1" || curStatus[0] == "2" || curStatus[0] == "3") {
                cache.incr("user_" + curStatus[0] + "_1_" + win, 1);
            }

            // 省份
            if (preStatus[1].isEmpty() && !curStatus[1].isEmpty()) {
                cache.incr("prov_" + curStatus[1] + "_" + win, 1);
                update[1] = curStatus[1];
            } else {
                cache.incr("prov_" + preStatus[1] + "_" + win, 1);
                update[1] = preStatus[1];
            }

            // 渠道
            if ((preStatus[2] == Constant.UNKNOWN || preStatus[2] == Constant.DIRECT)
                && (curStatus[2] != Constant.UNKNOWN && curStatus[2] != Constant.DIRECT)) {
                cache.incr("chnl_" + curStatus[2] + "_" + win, 1);
                update[2] = curStatus[2];
            } else {
                cache.incr("chnl_" + preStatus[2] + "_" +  win, 1);
                update[2] = preStatus[2];
            }
        } else {
            if (preStatus[1].isEmpty() && !curStatus[1].isEmpty()) {
                cache.incr("prov_" + curStatus[1] + "_" +  win, 1);
                cache.incr("prov_" + preStatus[1] + "_" +  win, -1);
                update[1] = curStatus[1];
                flag = true;
            }
            if ((preStatus[2].equals(Constant.UNKNOWN) || preStatus[2].equals(Constant.DIRECT))
                && (!curStatus[2].equals(Constant.UNKNOWN) && !curStatus[2].equals(Constant.DIRECT))) {
                cache.incr("chnl_" + curStatus[2] + "_" + win, 1);
                cache.incr("chnl_" + preStatus[2] + "_" + win, -1);
                update[2] = curStatus[2];
                flag = true;
            }
        }

        if (flag) {
            cache.put(statusKey, StringUtils.join(update, "_"));
        }
    }

    private void setPreState(String vals) {
        String[] s = StringUtils.splitPreserveAllTokens(vals, "_");

        for (int i = 0; i < 3; i++) {
            this.preStatus[i] = s[i];
        }
    }

    private void setPreState(String[] vals) {
        for (int i = 0; i < 3; i++) {
            this.preStatus[i] = vals[i];
        }
    }

    private void setCurState(String[] vals) {
        for (int i = 0; i < 3; i++) {
            this.curStatus[i] = vals[i];
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        try {
            cache = CachedFactory.getHBaseAsyncMapMemory();
            auxCache = CachedFactory.getChannelHBaseLRUReadonly(); // 维表 用于查询渠道
        } catch (IOException e) {
            throw new RuntimeException(e);
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
