package com.yihaodian.bi.hbase.dao;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;

public interface TrackerDao extends BaseDao {

    /**
     * 记录历史点数据
     * 
     * @param tableName 表名
     * @param rowkey rowkey
     * @param columnFamily 列族
     * @param qualifierArr 列数组
     * @param valueArr 值数组
     * @throws Exception
     */
    public void insertUvResult(String tableName, String rowkey, String columnFamily, String[] qualifierArr,
                               String[] valueArr) throws Exception;

    /**
     * 记录guid
     * 
     * @param tableName
     * @param rowkeyList
     * @param columnFamily
     * @param qualifier
     * @throws Exception
     */
    public void insertUvTmp(String tableName, List<String> rowkeyList, String columnFamily, String qualifier)
                                                                                                             throws Exception;

    /**
     * 统计分钟级uv表总行数 实现去重
     * 
     * @param tableName
     * @param rowPrefix rowkey前缀 分钟的reverse值
     * @return
     * @throws IOException
     */
    // public long getTotalUv(String tableName, String rowPrefix) throws Exception;

    /**
     * rowkey获取一行记录
     * 
     * @param tableName
     * @param rowkey
     * @return
     * @throws Exception
     */
    public Result getResult(String tableName, String rowkey) throws Exception;

}
