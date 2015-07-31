package com.yihaodian.bi.cached;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * TODO 类实现描述 线程安全
 * 
 * @author zhaoheng Jul 20, 2015 3:20:17 PM
 */
public class AsyncCommitCMap extends BaseCMap {

    @Override
    public String toString() {
        return "AsyncCommitCacheMap[ " + mps.toString() + "]";
    }

    public static Logger LOG = Logger.getLogger(AsyncCommitCMap.class);

    public static class AsyncCommitThread extends Thread {

        public static Logger LOG   = Logger.getLogger(AsyncCommitThread.class);
        boolean              isRun = true;
        AsyncCommitCMap      cached;
        int                  interval;
        Map<String, String>  tmpBatch;
        Map<String, Long>    tmpIncBatch;
        Exception            err;

        public AsyncCommitThread(AsyncCommitCMap buf, int interval){
            this.cached = buf;
            this.interval = interval;
        }

        @Override
        public void run() {
            while (isRun) {
                try {
                    Thread.sleep(interval);
                    if (cached.batchMap.size() > 0) {
                        synchronized (cached.batchMap) {
                            tmpBatch = cached.batchMap;
                            int batchSize = (int) (cached.batchMap.size() * 1.75F);
                            cached.batchMap = new HashMap(batchSize);
                        }
                        // LOG.info(cached.mps.toString() + " AsyncCommit BatchSize = " + tmpBatch.size());
                        cached.mps.puts(tmpBatch);
                        tmpBatch = null;
                    }
                    if (cached.batchIncMap.size() > 0) {
                        synchronized (cached.batchIncMap) {
                            int batchIncSize = (int) (cached.batchIncMap.size() * 1.75F);
                            tmpIncBatch = cached.batchIncMap;
                            cached.batchIncMap = new HashMap(batchIncSize);
                        }
                        // LOG.info(cached.mps.toString() + " AsyncCommit IncBatchSize = " + tmpIncBatch.size());
                        for (String k : tmpIncBatch.keySet()) {
                            cached.mps.incr(k, tmpIncBatch.get(k));
                        }
                        tmpIncBatch = null;
                    }
                } catch (Exception e) {
                    LOG.error(e);
                    err = e;
                    isRun = false;
                }
            }
        }
    }

    private Map<String, String> batchMap;
    private Map<String, Long>   batchIncMap;
    private AsyncCommitThread   async;
    private CMap                mps = null;

    public AsyncCommitCMap(CMap mps){
        this(mps, false, 0);
    }

    public AsyncCommitCMap(CMap mps, boolean asyncBatch, int asyncInterval){
        this.mps = mps;
        if (asyncBatch) {
            batchMap = new HashMap<String, String>();
            batchIncMap = new HashMap<String, Long>();
            async = new AsyncCommitThread(this, asyncInterval);
            async.start();
        }
    }

    public void stop() {
        async.isRun = false;
    }

    @Override
    public String get(String k) throws Exception {
        Object v;
        synchronized (batchMap) {
            v = batchMap.get(k);
            if (v != null) return (String) v;
        }
        return mps.get(k);
    }

    @Override
    public void put(String k, String v) throws Exception {
        if (!async.isRun) {
            throw new Exception("AsyncCommit Stopped", async.err);
        }
        synchronized (batchMap) {
            batchMap.put(k, v);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void puts(Map<String, String> mps) throws Exception {
        if (!async.isRun) {
            throw new Exception("AsyncCommit Stopped", async.err);
        }
        synchronized (batchMap) {
            for (String k : mps.keySet()) {
                batchMap.put(k, mps.get(k));
            }
        }
    }

    @Override
    public void incr(String k, long inc) throws Exception {
        if (!async.isRun) {
            throw new Exception("AsyncCommit Stopped", async.err);
        }
        Long v = null;
        synchronized (batchIncMap) {
            v = batchIncMap.get(k);
            if (v == null) {
                v = inc;
            } else {
                v = v + inc;
            }
            batchIncMap.put(k, v);
        }
    }
}
