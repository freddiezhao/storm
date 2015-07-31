package com.yihaodian.bi.view;

import java.util.Map;

public interface Representable {

    /**
     * 描绘索引值为key的点
     * 
     * @param tbl 表名
     * @param x 索引值
     * @return
     */
    public Map<?, ?> dot(String tbl, String key);

    /**
     * 描绘介于两个特定索引值之间的点
     * 
     * @param tbl 表名
     * @param keyStart 开始索引值
     * @param keyEnd 结束索引值
     * @return
     */
    public Map<?, ?> curve(String tbl, String startRow, String endRow);

}
