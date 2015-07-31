package com.yihaodian.bi.cached.factory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.map.LRUMap;

import com.yihaodian.bi.cached.AsyncCommitCMap;
import com.yihaodian.bi.cached.BufferedReadCMap;
import com.yihaodian.bi.cached.CMap;
import com.yihaodian.bi.cached.MemoryCMap;
import com.yihaodian.bi.common.Debug;
import com.yihaodian.bi.hbase.HBaseConstant;

public class CachedFactory {

    private static CMap defaultHbaseAsyncLRU    = null;
    private static CMap channelHbaseLRUReadonly = null;
    private static CMap defaultHbaseAsyncMem    = null;

    public static int   CACHE_SIZE              = 100000;
    public static int   ASYNC_INTERVAL          = 50;

    public synchronized static CMap getDefaultHBase() throws IOException {
        return HBaseFactory.getHBaseCMap(HBaseConstant.Table.CHANNEL_KV_TABLE);
    }

    public synchronized static CMap getChannelHBaseLRUReadonly() throws IOException {
        if (channelHbaseLRUReadonly == null) {
            if (!Debug.DEBUG) {
                channelHbaseLRUReadonly = new BufferedReadCMap(
                                                               new MemoryCMap(new LRUMap(CACHE_SIZE)),
                                                               HBaseFactory.getHBaseCMap(HBaseConstant.Table.CHANNEL_KV_TABLE));
            } else {
                channelHbaseLRUReadonly = new MemoryCMap(new LRUMap(CACHE_SIZE));
            }
        }
        return channelHbaseLRUReadonly;
    }

    public synchronized static CMap getDefaultHBaseAsyncLRU() throws IOException {
        if (defaultHbaseAsyncLRU == null) {
            if (!Debug.DEBUG) {
                defaultHbaseAsyncLRU = new AsyncCommitCMap(
                                                           new BufferedReadCMap(
                                                                                new MemoryCMap(new LRUMap(CACHE_SIZE)),
                                                                                HBaseFactory.getHBaseCMap(HBaseConstant.Table.DEFAULT_KV_TABLE)),
                                                           true, ASYNC_INTERVAL);
            } else {
                defaultHbaseAsyncLRU = new AsyncCommitCMap(new MemoryCMap(new LRUMap(CACHE_SIZE)), true, ASYNC_INTERVAL);
            }
        }
        return defaultHbaseAsyncLRU;
    }

    public synchronized static CMap getHBaseAsyncMapMemory() throws IOException {
        if (defaultHbaseAsyncMem == null) {
            if (!Debug.DEBUG) {
                defaultHbaseAsyncMem = new AsyncCommitCMap(
                                                           new BufferedReadCMap(
                                                                                new MemoryCMap(new ConcurrentHashMap()),
                                                                                HBaseFactory.getHBaseCMap(HBaseConstant.Table.DEFAULT_KV_TABLE)),
                                                           true, ASYNC_INTERVAL);
            } else {
                defaultHbaseAsyncMem = new AsyncCommitCMap(new MemoryCMap(new LRUMap(CACHE_SIZE)), true, ASYNC_INTERVAL);
            }
        }
        return defaultHbaseAsyncMem;
    }
}
