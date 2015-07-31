package com.yihaodian.bi.cached;

import java.util.Map;

import com.yihaodian.bi.cached.factory.YCacheFactory;
import com.yihaodian.bi.common.Debug;
import com.yihaodian.common.ycache.CacheProxy;

/**
 * 类YCacheMap.java的实现描述：TODO 类实现描述
 * 
 * @author zhaoheng Jul 22, 2015 11:52:47 AM
 */
public class YCacheCMap extends Debug implements CMap {

    protected CacheProxy cache = YCacheFactory.getCache();

    @Override
    public String get(String k) {
        return (String) cache.get(k);
    }

    @Override
    public String toString() {
        return "YCacheMap";
    }

    @Override
    public void put(String k, String v) {
        cache.put(k, v);
    }

    @Override
    public void puts(Map<String, String> mps) {
        for (String k : mps.keySet()) {
            cache.put(k, mps.get(k));
        }
    }

    @Override
    public void incr(String k, long inc) {
        cache.incr(k, inc);
    }

    @Override
    public Long getLong(String k) throws Exception {
        return cache.incr(k, 0);
    }
}
