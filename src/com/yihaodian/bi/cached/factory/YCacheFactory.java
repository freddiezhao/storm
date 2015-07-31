package com.yihaodian.bi.cached.factory;

import org.apache.log4j.Logger;

import com.yihaodian.common.ycache.CacheProxy;
import com.yihaodian.common.ycache.memcache.YmemcacheProxyFactory;
import com.yihaodian.common.ycache.memcache.exception.MemcacheInitException;

public class YCacheFactory {

    private static Logger     LOG   = Logger.getLogger(YCacheFactory.class);
    private static CacheProxy cache = null;

    static {
        try {
            YmemcacheProxyFactory.configure("memcache.xml");
        } catch (MemcacheInitException e) {
            LOG.error(e);
            throw new RuntimeException(e);
        }
    }

    public synchronized static CacheProxy getCache() {
        if (cache == null) {
            cache = YmemcacheProxyFactory.getClient("bi_storm");
        }
        return cache;
    }
}
