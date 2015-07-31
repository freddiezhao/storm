package com.yihaodian.bi.hbase;

import java.util.ArrayList;
import java.util.List;

/**
 * 类HBaseConstant.java的实现描述：TODO 类实现描述
 * 
 * @author zhaoheng Jul 28, 2015 4:54:01 PM
 */
public class HBaseConstant {

    /**
     * 普通表
     */
    public static final int TABLE_TYPE_DEFAULT   = 0;

    /**
     * 维表
     */
    public static final int TABLE_TYPE_DIMENSION = 10;

    public static enum Table {
        CHANNEL_KV_TABLE("bi_dim_chnl", TABLE_TYPE_DIMENSION), 
        DEFAULT_KV_TABLE("bi_uv_cache", TABLE_TYPE_DEFAULT);

        public String tbName;
        public int    type;

        Table(String tbName, int type){
            this.tbName = tbName;
            this.type = type;
        }

        public static List<Table> listByType(int type) {
            List<Table> ts = new ArrayList<Table>(Table.values().length);
            for (Table t : Table.values()) {
                if (t.type == type) {
                    ts.add(t);
                }
            }
            return ts;
        }
    }
}
