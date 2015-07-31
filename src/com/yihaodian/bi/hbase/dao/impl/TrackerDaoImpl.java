package com.yihaodian.bi.hbase.dao.impl;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;

import com.yihaodian.bi.hbase.dao.TrackerDao;

public class TrackerDaoImpl extends BaseDaoImpl implements TrackerDao {

	public TrackerDaoImpl() throws IOException {
		super();
	}

	@Override
	public void insertUvResult(String tableName, String rowkey,
			String columnFamily, String[] qualifierArr, String[] valueArr)
			throws Exception {
		super.insertRecord(tableName, rowkey, columnFamily, qualifierArr,
				valueArr);
	}

	@Override
	public void insertUvTmp(String tableName, List<String> rowkeyList,
			String columnFamily, String qualifier) throws Exception {
		super.insertRowkeys(tableName, rowkeyList, columnFamily, qualifier);
	}

	@Override
	public Result getResult(String tableName, String rowkey) throws Exception {
		return super.getOneRecord(tableName, rowkey);
	}

	@Override
	public void insertRowkeys(String tableName, List<String> rowkeyList,
			String columnFamily, String qualifierArr, boolean wal,
			boolean autoFlush, long writeBuffer) throws IOException {
		// TODO Auto-generated method stub
		super.insertRowkeys(tableName, rowkeyList, columnFamily, qualifierArr, wal,
				autoFlush, writeBuffer);
	}
}
