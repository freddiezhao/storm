package com.yihaodian.bi.cached;

import java.util.Map;

public class MemoryCMap extends BaseCMap {

    Map<String, String> map;

    public MemoryCMap(Map<String, String> map){
        this.map = map;
    }

    @Override
    public String get(String k) {
        return (String) map.get(k);
    }

    @Override
    public void put(String k, String v) {
        map.put(k, v);
    }

    @Override
    public void puts(Map mps) {
        map.putAll(mps);
    }

    @Override
    public void incr(String k, long inc) throws Exception {
        Long v = getLong(k);
        if (v != null) {
            map.put(k, String.valueOf(v + inc));
        } else {
            map.put(k, String.valueOf(inc));
        }
    }
}
