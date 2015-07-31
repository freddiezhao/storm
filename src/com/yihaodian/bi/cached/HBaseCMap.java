package com.yihaodian.bi.cached;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.yihaodian.bi.cached.factory.HBaseFactory;
import com.yihaodian.bi.common.Debug;
import com.yihaodian.bi.hbase.HBaseConstant.Table;

/**
 * 类HBaseMap.java的实现描述：TODO 类实现描述
 */
public class HBaseCMap extends Debug implements CMap {

    HTable                     table = null;
    public static final byte[] CF    = "cf".getBytes();
    public static final byte[] QL    = "value".getBytes();

    public HBaseCMap(Table tb) throws IOException{
        if (!DEBUG) table = HBaseFactory.getTable(tb);
    }

    @Override
    public String get(String k) throws Exception {
        byte[] v = table.get(new Get(Bytes.toBytes(k))).getValue(CF, QL);
        if (v == null) {
            return null;
        } else {
            return new String(v);
        }
    }

    @Override
    public void put(String k, String v) throws Exception {
        Put p = new Put(Bytes.toBytes(k));
        p.add(CF, QL, Bytes.toBytes(v));
        table.put(p);
    }

    @Override
    public String toString() {
        return "HBaseMap";
    }

    @Override
    public void puts(Map<String, String> mps) throws Exception {
        List<Put> ps = new ArrayList<Put>();
        for (String k : mps.keySet()) {
            Put p = new Put(Bytes.toBytes(k));
            p.add(CF, QL, Bytes.toBytes(mps.get(k)));
            ps.add(p);
        }
        table.put(ps);
    }

    @Override
    public void incr(String k, long inc) throws IOException {
        table.incrementColumnValue(Bytes.toBytes(k), CF, QL, inc);
    }

    @Override
    public Long getLong(String k) throws Exception {
        return table.incrementColumnValue(Bytes.toBytes(k), CF, QL, 0);
    }
}
