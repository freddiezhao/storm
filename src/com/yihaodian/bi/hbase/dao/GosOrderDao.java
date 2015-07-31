package com.yihaodian.bi.hbase.dao;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;

import com.yihaodian.bi.storm.common.model.CharDataVO;

public interface GosOrderDao extends BaseDao {

	public void createTestTable(String tableName, String[] columnFamilys)
			throws Exception;

	public void insertGosOrder(String tableName, String rowkey,
			String columnFamily, String[] qualifierArr, String[] valueArr)
			throws Exception;

	public void insertGosOrder(String tableName, String rowkey,
			String columnFamily, String qualifier, String value)
			throws Exception;

	public Result getGosOrder(String tableName, String rowkey)
			throws IOException;

	public String getValue(String tableName, String rowkey) throws Exception;

	public String getValueByCol(String tableName, String rowkey, String colName)
			throws Exception;

	public List<Result> findHistory(String tableName, String rowKeyRegex,
			CharDataVO columnname) throws Exception;

	public boolean exists(String tableName, String string) throws IOException;
}
