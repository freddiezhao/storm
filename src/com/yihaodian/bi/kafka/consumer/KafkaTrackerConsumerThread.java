package com.yihaodian.bi.kafka.consumer;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yihaodian.bi.cached.CMap;
import com.yihaodian.bi.cached.factory.CachedFactory;
import com.yihaodian.bi.common.Debug;
import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.kafka.TrackerVO;

/**
 * 类TrackerBatchConsumer.java的实现描述：TODO 类实现描述
 * 
 * @author zhaoheng Jul 20, 2015 10:23:55 AM
 */
public class KafkaTrackerConsumerThread extends Thread {

    public static final String KAFKATOPIC                       = "tracker";
    public static final Logger LOG                              = LoggerFactory.getLogger(KafkaTrackerConsumerThread.class);
    TrackerVO                  tracker                          = null;
    BlockingQueue<TrackerVO>   queue                            = new LinkedBlockingDeque<TrackerVO>(100);
    CMap                       cache;
    KafkaTrackerConsumer       consumer;

    // 统计数据
    private long               lastPoint                        = DateUtil.getLastCheckPointWholeFiveMinute();
    long                       stat_filterColumnsSizeLessThan30 = 0;
    long                       stat_filterBaiduSpider           = 0;
    long                       stat_filterUrlNonYHD             = 0;
    long                       stat_filterUrlUnion              = 0;
    long                       stat_filterButton                = 0;
    long                       stat_AllCount                    = 0;
    boolean                    isRun                            = false;
    Pattern                    p                                = Pattern.compile("^https?://[^/]*yhd\\.com.*");

    @Override
    public String toString() {
        return "stat_filterColumnsSizeLessThan30=" + stat_filterColumnsSizeLessThan30 + ", stat_filterUrlNonYHD="
               + stat_filterUrlNonYHD + ", stat_filterUrlUnion=" + stat_filterUrlUnion + ", stat_filterButton="
               + stat_filterButton + ", stat_AllCount=" + stat_AllCount;
    }

    public KafkaTrackerConsumerThread(String groupId, boolean isRun) throws IOException{
        this(groupId, KAFKATOPIC, isRun);
    }

    public KafkaTrackerConsumerThread(String groupId) throws IOException{
        this(groupId, KAFKATOPIC, false);
    }

    KafkaTrackerConsumerThread(String groupId, String topic, boolean isRun) throws IOException{
        init(groupId, topic);
        this.isRun = isRun;
    }

    public void writeStats() throws Exception {
        long cur = System.currentTimeMillis();
        if (cur - lastPoint > DateUtil.MINUTE_FIVE) {
            String dt = DateUtil.YYYYMMDDHHMM.format(new Date(cur));
            cache.incr("tracker_cum_all_count_" + dt, stat_AllCount);
            cache.incr("tracker_cum_f_cols_lt30_" + dt, stat_filterColumnsSizeLessThan30);
            cache.incr("tracker_cum_f_baidu_" + dt, stat_filterBaiduSpider);
            cache.incr("tracker_cum_f_urlnonyhd_" + dt, stat_filterUrlNonYHD);
            cache.incr("tracker_cum_f_urlunion_" + dt, stat_filterUrlUnion);
            cache.incr("tracker_cum_f_button_" + dt, stat_filterButton);
            stat_filterColumnsSizeLessThan30 = 0;
            stat_filterBaiduSpider = 0;
            stat_filterUrlNonYHD = 0;
            stat_filterUrlUnion = 0;
            stat_filterButton = 0;
            stat_AllCount = 0;
            lastPoint = cur;
        }
    }

    public BlockingQueue<TrackerVO> getQueue() {
        return queue;
    }

    private void init(String groupId, String topic) throws IOException {
        setDaemon(true);
        if (Debug.DEBUG) {
            consumer = new OfflineConsumer("sample_1k.dat.zip");
        } else {
            consumer = new OnlineKafkaTrackerConsumer(groupId, topic);
        }
        cache = CachedFactory.getDefaultHBaseAsyncLRU();
    }

    public void run() {
        while (consumer.hasNext()) {
            while (!isRun) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
            }

            String s = consumer.next();
            stat_AllCount++;

            // 过滤来自百度的爬虫数据
            if (s.contains("http://www.baidu.com/search/spider.html")) {
                stat_filterBaiduSpider++;
                continue;
            }
            String cols[] = s.split("\\t");
            // 列小于30的丢弃
            if (cols.length < 30) {
                stat_filterColumnsSizeLessThan30++;
                continue;
            }
            // 过滤URL
            if (!StringUtils.isEmpty(cols[1])) {// 为空的不处理
                Matcher m = p.matcher(cols[1]);
                if (!m.find()) {
                    stat_filterUrlNonYHD++;
                    continue;
                }
            }

            if (cols[1].startsWith("http://union.yihaodian.com/link_make/viewPicInfo.do")
                || cols[1].startsWith("http://union.yhd.com/resourceCenter/viewSearchBox.do")
                || cols[1].startsWith("http://union.yhd.com/resourceCenter/viewRanking.do")
                || cols[1].startsWith("http://union.yhd.com/resourceCenter/getUserCookies.do")
                || cols[1].startsWith("http://union.yhd.com/resourceCenter/viewShoppingWindow.do")
                || cols[1].startsWith("http://union.yhd.com/resourceCenter/getUserCookies.do")) {
                stat_filterUrlUnion++;
                continue;
            }

            tracker = new TrackerVO(cols);
            if (tracker.getButton_position() != null && !"null".equals(tracker.getButton_position())
                && tracker.getButton_position().length() > 1) {
                stat_filterButton++;
                continue;
            }
            try {
                queue.put(tracker);
                writeStats();
            } catch (Exception e) {
            }
        }
        if (Debug.DEBUG) System.out.println("KafkaTrackerConsumerThread stopped....");
    }

    public long getStat_filterColumnsSizeLessThan30() {
        return stat_filterColumnsSizeLessThan30;
    }

    public long getStat_filterBaiduSpider() {
        return stat_filterBaiduSpider;
    }

    public long getStat_filterUrlNonYHD() {
        return stat_filterUrlNonYHD;
    }

    public long getStat_filterUrlUnion() {
        return stat_filterUrlUnion;
    }

    public long getStat_filterButton() {
        return stat_filterButton;
    }

    public long getStat_AllCount() {
        return stat_AllCount;
    }

    public void setIsRun(boolean b) {
        isRun = b;
    }

}
