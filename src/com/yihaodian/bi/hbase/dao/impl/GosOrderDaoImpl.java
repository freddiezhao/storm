package com.yihaodian.bi.hbase.dao.impl;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.storm.common.model.CharDataVO;

public class GosOrderDaoImpl extends BaseDaoImpl implements GosOrderDao {

	public GosOrderDaoImpl() throws IOException {
		super();
	}

	public void createTestTable(String tableName, String[] columnFamilys)
			throws Exception {
		super.createTable(tableName, columnFamilys);
	}

	@Override
	public void insertGosOrder(String tableName, String rowkey,
			String columnFamily, String qualifier, String value)
			throws Exception {
		super.insertRecord(tableName, rowkey, columnFamily, qualifier, value);
	}

	@Override
	public void insertGosOrder(String tableName, String rowkey,
			String columnFamily, String[] qualifierArr, String[] valueArr)
			throws Exception {
		super.insertRecord(tableName, rowkey, columnFamily, qualifierArr,
				valueArr);
	}

	@Override
	public Result getGosOrder(String tableName, String rowkey)
			throws IOException {
		return super.getOneRecord(tableName, rowkey);
	}

	/**
	 * 获取记录值
	 * 
	 * @param tableName
	 * @param rowkey
	 * @return
	 */
	public String getValue(String tableName, String rowkey) throws Exception {
		String value = "";
		try {
			Result result = this.getGosOrder(tableName, rowkey);
			for (KeyValue keyValue : result.raw()) {
				value = new String(keyValue.getValue());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}

	@Override
	public List<Result> findHistory(String tableName, String rowKeyRegex,
			CharDataVO columnname) throws Exception {
		List<Result> list = new LinkedList<Result>();
		try {
			list = super.getRecordByRowKeyRegex(tableName, rowKeyRegex,
					columnname.getColumns());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return list;
	}

	@Override
	public String getValueByCol(String tableName, String rowkey, String colName)
			throws Exception {
		String value = "";
		try {
			Result result = this.getGosOrder(tableName, rowkey);
			if (result != null) {
				byte[] b = result.getValue("cf".getBytes(), colName.getBytes());
				if (b != null) {
					value = new String(b);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}
	
	/**
	 * 
	 * @param tableName
	 * @param rowKey
	 * @return
	 * @throws IOException
	 */
	@Override
	public boolean exists(String tableName, String rowKey) throws IOException {
		HTable table = new HTable(configuration, tableName);
		Get get = new Get(rowKey.getBytes());
		boolean res = table.exists(get);
		table.close();
		
		return res;
	}

}
