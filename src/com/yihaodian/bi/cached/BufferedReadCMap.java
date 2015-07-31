package com.yihaodian.bi.cached;

import java.util.Map;

import org.apache.log4j.Logger;

/**
 * 类BufferedCachedMaps.java的实现描述：TODO 类实现描述 非线程安全，注意不要线程共享该对象
 * 
 * @author zhaoheng Jul 20, 2015 3:20:17 PM
 */
public class BufferedReadCMap extends BaseCMap {

    @Override
    public String toString() {
        return "buf [" + buffered.toString() + "] -> [" + mps.toString() + "]";
    }

    public static Logger LOG      = Logger.getLogger(BufferedReadCMap.class);
    protected CMap       buffered = null;
    protected CMap       mps      = null;

    public BufferedReadCMap(CMap buffered, CMap mps){
        this.buffered = buffered;
        this.mps = mps;
    }

    @Override
    public String get(String k) throws Exception {
        if (buffered.get(k) != null) {
            return buffered.get(k);
        }
        String obj = mps.get(k);
        if (obj != null) {
            buffered.put(k, obj);
            return obj;
        }
        return null;
    }

    @Override
    public void put(String k, String v) throws Exception {
        buffered.put(k, v);
        mps.put(k, v);
    }

    @Override
    public void puts(Map<String, String> mps) throws Exception {
        buffered.puts(mps);
        this.mps.puts(mps);
    }

    @Override
    public void incr(String k, long inc) throws Exception {
        buffered.incr(k, inc);
        mps.incr(k, inc);
    }
}
