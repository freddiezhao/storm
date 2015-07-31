package com.yihaodian.bi.cached.factory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import com.yihaodian.bi.cached.CMap;
import com.yihaodian.bi.cached.HBaseCMap;
import com.yihaodian.bi.hbase.HBaseConstant.Table;

/**
 * 类HBaseFactory.java的实现描述：TODO 类实现描述 
 * @author zhaoheng Jul 28, 2015 4:53:52 PM
 */
public class HBaseFactory {

    protected static final Configuration       CONF            = HBaseConfiguration.create();
    protected static final Map<String, CMap>   CMPS            = new HashMap<String, CMap>();
    protected static final Map<String, HTable> TABLES          = new HashMap<String, HTable>();

    public synchronized static HTable getTable(Table tb) throws IOException {
        if (TABLES.get(tb.tbName) != null) {
            return TABLES.get(tb.tbName);
        } else {
            HTable table = new HTable(CONF, tb.tbName);
            table.setAutoFlush(true, false);
            TABLES.put(tb.tbName, table);
            return table;
        }
    }

    public synchronized static CMap getHBaseCMap(Table tb) throws IOException {
        if (CMPS.get(tb.tbName) != null) {
            return CMPS.get(tb.tbName);
        } else {
            CMap table = new HBaseCMap(tb);
            CMPS.put(tb.tbName, table);
            return table;
        }
    }
}
