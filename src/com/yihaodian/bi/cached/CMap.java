package com.yihaodian.bi.cached;

import java.util.Map;

/**
 * 类MapCached.java的实现描述：TODO 类实现描述
 * 
 * @author zhaoheng Jul 20, 2015 2:29:14 PM
 */
public interface CMap {

    public String get(String k) throws Exception;

    public void put(String k, String v) throws Exception;

    public void puts(Map<String, String> mps) throws Exception;

    public void incr(String k, long inc) throws Exception;

    public Long getLong(String k) throws Exception;
}
