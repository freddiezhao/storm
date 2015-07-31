package com.yihaodian.bi.storm.logic.tracker.common;

import com.yihaodian.bi.cached.CMap;
import com.yihaodian.bi.kafka.TrackerVO;
import com.yihaodian.bi.storm.common.util.Constant;

public class Utils {

    /**
     * 判断平台类型 TODO 确定类型 0 app 1 h5
     * 
     * @param url
     * @param container
     * @return
     */
    public static String pltmType(TrackerVO t) {
        String platform = t.getPlatform();
        String container = t.getContainer();
        String url = t.getUrl();

        if (platform != null) {
            platform = platform.toLowerCase();
        }
        if (platform != null && container != null) {
            if (container.equals("yhdapp") && (platform.indexOf("iossystem") != -1 || platform.endsWith("iphone"))) {
                return "1";
            } else if (container.equals("yhdapp")
                       && (platform.indexOf("ipadsystem") != -1 || platform.endsWith("ipad"))) {
                return "2";
            } else if (container.equals("yhdapp")
                       && (platform.indexOf("androidsystem") != -1 || platform.endsWith("android"))) {
                return "3";
            } else if ((container.equals("H5") || (!container.equals("yhd-im-pc") && !container.isEmpty())
                                                  && (platform.indexOf("androidsystem") != -1 || platform.indexOf("iossystem") != -1))) {
                return "4";
            }
        }

        if (platform != null && url != null) {
            if (!url.isEmpty()
                && (url.indexOf("http://m.") == -1)
                && (platform.indexOf("iossystem") != -1 || platform.indexOf("ipadsystem") != -1
                    || platform.indexOf("androidsystem") != -1 || platform.endsWith("iphone")
                    || platform.endsWith("ipad") || platform.endsWith("android"))) {
                return "4";
            } else if (url.startsWith("http://m.yhd.com")
                       || (url.startsWith("http://m.yihaodian.com") && url.indexOf("/wm/") != -1)
                       || url.indexOf(".m.yhd.com") != -1) {
                return "4";
            }
        }

        return "-1";
    }

    /**
     * 获得渠道 查cache
     * 
     * @param dao
     * @param t
     * @return
     * @throws Exception
     */
    public static String channel(CMap cache, TrackerVO t) throws Exception {
        String tracker_u = t.getTracker_u();
        if (tracker_u != null && !tracker_u.isEmpty()) {
            String res = cache.get(tracker_u);
            if (res == null) {
                return Constant.UNKNOWN;
            } else {
                return res;
            }
        }
        String referer = t.getReferer();
        if (referer != null && !referer.isEmpty()) {
            if (referer.indexOf(".baidu.com/") != -1 || referer.startsWith("http://so.360.cn/")
                || referer.startsWith("http://www.so.com/") || referer.startsWith("http://m.so.com/")
                || referer.startsWith("http://www.haosou.com/") || referer.startsWith("http://www.google.")
                || referer.startsWith("https://www.google.") || referer.indexOf(".soso.com/") != -1
                || referer.startsWith("http://www.sogou.com/") || referer.startsWith("http://gouwu.youdao.com/search")
                || referer.indexOf(".bing.com/") != -1 || referer.startsWith("http://www.youdao.com/search?q=")
                || referer.indexOf(".yahoo.com/") != -1 || referer.indexOf(".yahoo.cn/") != -1
                || referer.indexOf("http://glb.uc.cn/") != -1 || referer.indexOf(".sm.cn/") != -1
                || referer.startsWith("http://www.yhd.com/s-theme/")
                || referer.startsWith("http://www.yhd.com/S-theme/")
                || referer.startsWith("http://www.yhd.com/marketing/hs-")) {
                return Constant.SEO;
            }
        } else {
            return Constant.DIRECT;
        }

        return Constant.UNKNOWN;
    }

}
