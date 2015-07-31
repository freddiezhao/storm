package com.yihaodian.bi.cached;

public abstract class BaseCMap implements CMap {

    @Override
    public Long getLong(String k) throws Exception {
        String v = this.get(k);
        if (v == null) {
            return null;
        } else {
            try {
                return Long.valueOf(v);
            } catch (Exception e) {
                return null;
            }
        }
    }
}
