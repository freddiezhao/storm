package com.yihaodian.bi.hbase.dao;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

public interface BaseDao {

	public void createTable(String tableName, String[] columnFamilys)
			throws IOException;

	public void deleteTable(String tableName) throws IOException;

	public void insertRecord(String tableName, String rowkey,
			String columnFamily, String qualifier, String value)
			throws IOException;

	public void insertRecord(String tableName, String rowkey,
			String columnFamily, String[] qualifierArr, String[] valueArr)
			throws IOException;

	public void insertRowkeys(String tableName, List<String> rowkeyList,
			String columnFamily, String qualifierArr) throws IOException;

	/**
	 * 带有hbase写入性能参数的insert方法
	 * 
	 * @param tableName
	 * @param rowkeyList
	 * @param columnFamily
	 * @param qualifierArr
	 * @param wal
	 *            是否在插入前写日志
	 * @param autoFlush
	 *            是否自动提交 配合writeBuffer参数
	 * @param writeBuffer
	 *            当autoFlush为false时 自动提交的大小
	 * @throws IOException
	 */
	public void insertRowkeys(String tableName, List<String> rowkeyList,
			String columnFamily, String qualifierArr, boolean wal,
			boolean autoFlush, long writeBuffer) throws IOException;

	public void deleteRecord(String tableName, String rowkey)
			throws IOException;

	public Result getOneRecord(String tableName, String rowkey)
			throws IOException;

	public List<Result> getAllRecord(String tableName) throws IOException;

	public List<Result> getRecordByRange(String tableName, String startRow,
			String stopRow) throws IOException;
	
	public List<Result> getRecordByQualifier(String tableName,
			String columnFamily, String qualifier, String columnValue)
			throws IOException;

	public String getColumnValue(String tableName, String rowKey,
			String columnFamily, String qualifier)
			throws IOException;
	/**
	 * 模糊匹配 获取全列结果集
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKeyRegex
	 *            匹配项
	 * @return
	 * @throws IOException
	 */
	public List<Result> getRecordByRowKeyRegex(String tableName,
			String rowKeyRegex) throws IOException;

	/**
	 * 模糊匹配 获取指定列数的结果集
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKeyRegex
	 *            匹配项
	 * @param cols
	 *            列名数组
	 * @return
	 * @throws IOException
	 */
	public List<Result> getRecordByRowKeyRegex(String tableName,
			String rowKeyRegex, String[] cols) throws IOException;

}
