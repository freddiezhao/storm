package com.yihaodian.bi.hbase.dao.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import com.yihaodian.bi.hbase.dao.BaseDao;
import com.yihaodian.bi.storm.common.util.Constant;

public class BaseDaoImpl implements BaseDao {

	public String daoMsg;

	public Configuration configuration;


	private HBaseAdmin admin;

	public BaseDaoImpl() throws IOException {
		getConfiguration();
	}

	/**
	 * 初始化hbase基本配置
	 * 
	 * @return Configuration
	 */
	public Configuration getConfiguration() throws IOException {
		configuration = HBaseConfiguration.create();
		this.admin = new HBaseAdmin(this.configuration);
//		String filePath = "hbase-site.xml";// 发现这里没用，默认读取src目录下的hbase-site.xml文件
//		Path path = new Path(filePath);
//		configuration.addResource(path);
		return configuration;
	}

	/**
	 * 创建hbase表
	 * 
	 * @parma:tableName 表名 columnFamily 列族
	 */
	@Override
	public void createTable(String tableName, String[] columnFamilys)
			throws IOException {
		if (admin.tableExists(tableName)) {
			daoMsg = "添加失败,表'" + tableName + "'已经存在!";
			throw new IOException("添加失败,表'" + tableName + "'已经存在!");
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		/* 开始遍历列族 */
		for (int i = 0; i < columnFamilys.length; i++) {
			tableDescriptor.addFamily(new HColumnDescriptor(columnFamilys[i]));
		}
		admin.createTable(tableDescriptor);
	}

	/**
	 * 删除hbase表
	 * 
	 * @parma:tableName
	 */
	@Override
	public void deleteTable(String tableName) throws IOException {
		if (this.admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			daoMsg = "删除表'" + tableName + "'成功!";
		} else {
			daoMsg = "表'" + tableName + "'不存在!";
		}
	}

	/**
	 * 往hbase表插入数据 parma:tableName 表名 rowKey rowKey columnFamily 列族 column 列名
	 * value 值
	 */
	@Override
	public void insertRecord(String tableName, String rowkey,
			String columnFamily, String qualifier, String value)
	{
		HTable table = null;
		try {
			Put put = new Put(rowkey.getBytes());
			put.setWriteToWAL(true);// 关闭写日志 系统出现故障会导致数据丢失4444
			put
					.add(columnFamily.getBytes(), qualifier.getBytes(), value
							.getBytes());
			table = new HTable(configuration, tableName);
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}finally
		{
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		
	}

	/**
	 * 往hbase表批量插入数据 parma:tableName 表名 rowKey rowKey columnFamily 列族 column 列名
	 * value 值
	 */
	@Override
	public void insertRecord(String tableName, String rowkey,
			String columnFamily, String[] qualifierArr, String[] valueArr)
			throws IOException {
		Put put = new Put(rowkey.getBytes());
		// 批量添加
		for (int i = 0; i < qualifierArr.length; i++) {
			String col = qualifierArr[i];
			String val = valueArr[i];
			put.add(columnFamily.getBytes(), col.getBytes(), val.getBytes());
		}
		HTable table = new HTable(configuration, tableName);
		table.put(put);

		table.close();
	}

	/**
	 * by lvpeng 2014-5-5 往hbase表批量插入数据，批量rowkey ,value和rowkey同
	 */
	@Override
	public void insertRowkeys(String tableName, List<String> rowkeyList,
			String columnFamily, String qualifierArr) throws IOException {
		HTable table = new HTable(configuration, tableName);
		List<Put> putList = new ArrayList<Put>();

		for (int i = 0; i < rowkeyList.size(); i++) {
			Put put = new Put(rowkeyList.get(i).getBytes());
			put.add(columnFamily.getBytes(), qualifierArr.getBytes(),
					rowkeyList.get(i).getBytes());
			putList.add(put);
		}
		table.put(putList);
		table.close();
	}

	/**
	 * by zhangsheng 2014-5-7 批量插入 控制插入性能
	 */
	@Override
	public void insertRowkeys(String tableName, List<String> rowkeyList,
			String columnFamily, String qualifierArr, boolean wal,
			boolean autoFlush, long writeBuffer) throws IOException {
		HTable table = new HTable(configuration, tableName);
		table.setAutoFlush(autoFlush);
		if (writeBuffer != 0) {
			table.setWriteBufferSize(writeBuffer);
		}
		List<Put> putList = new ArrayList<Put>();

		for (int i = 0; i < rowkeyList.size(); i++) {
			Put put = new Put(rowkeyList.get(i).getBytes());
			put.add(columnFamily.getBytes(), qualifierArr.getBytes(),
					rowkeyList.get(i).getBytes());
			put.setWriteToWAL(wal);// 关闭写日志 系统出现故障会导致数据丢失
			putList.add(put);
		}
		table.put(putList);
		//add by lvpeng 2014-05-27   提交更新
		table.flushCommits() ;
		table.close();
	}

	/**
	 * 删除行 parma:tableName 表名 rowKey rowKey value 值
	 */
	@Override
	public void deleteRecord(String tableName, String rowkey)
			throws IOException {
		HTable table = new HTable(configuration, tableName);
		Delete del = new Delete(rowkey.getBytes());
		table.delete(del);
		daoMsg = "删除行成功";
	}
	
	/**
	 * 删除指定前缀的记录
	 * 
	 * @param tableName 表名
	 * @param rowKeyPrefix 行索引前缀
	 * @throws IOException
	 */
	public void deleteAll(String tableName, String rowKeyPrefix) throws IOException {
		
		HTable table = new HTable(configuration, tableName);
		PrefixFilter filter = new PrefixFilter(rowKeyPrefix.getBytes());
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		List<Delete> delList =  new ArrayList<Delete>();
		for (Result r : scanner) {
			 delList.add(new Delete(r.getRow()));
		}
		table.delete(delList);
		
		scanner.close();
		table.close();
		
	}
	/**
	 * 获取一行数据 parma:tableName 表名 rowKey
	 * 
	 */
	@Override
	public Result getOneRecord(String tableName, String rowkey)
			throws IOException {
		HTable table = new HTable(configuration, tableName);
		Get get = new Get(rowkey.getBytes());
		Result rs = table.get(get);
		table.close();
		return rs;
	}

	/**
	 * 获取所有数据 parma:tableName 表名
	 * 
	 */
	@Override
	public List<Result> getAllRecord(String tableName) throws IOException {
		HTable table = new HTable(this.configuration, tableName);
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		List<Result> list = new ArrayList<Result>();
		for (Result r : scanner) {
			list.add(r);
		}
		scanner.close();
		table.close();
		return list;
	}

	/**
	 * 获取范围内数据 parma:tableName 表名 startRow 开始行 stopRow 结束行 select
	 * Id,END_USER_ID,ORDER_AMOUNT from so where row_key between ... and
	 */
	@Override
	public List<Result> getRecordByRange(String tableName, String startRow,
			String stopRow) throws IOException {
		HTable table = new HTable(configuration, tableName);
		Scan scan = new Scan();
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());
		ResultScanner scanner = table.getScanner(scan);
		List<Result> list = new ArrayList<Result>();
		for (Result r : scanner) {
			list.add(r);
		}
		scanner.close();
		table.close();
		return list;
	}

	/**
	 * 通过列的值匹配数据 parma:tableName 表名 columnFamily 列族 qualifier 列名 columnValue 值
	 * select .... from so where end_user_id=?
	 */
	@Override
	public List<Result> getRecordByQualifier(String tableName,
			String columnFamily, String qualifier, String columnValue)
			throws IOException {
		HTable table = new HTable(configuration, tableName);
		Scan scan = new Scan();
		Filter filter = new SingleColumnValueFilter(columnFamily.getBytes(),
				qualifier.getBytes(), CompareFilter.CompareOp.EQUAL,
				columnValue.getBytes());
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		List<Result> list = new ArrayList<Result>();
		for (Result r : scanner) {
			list.add(r);
		}
		scanner.close();
		table.close();
		return list;
	}
	
	public String getColumnValue(String tableName, String rowKey,String columnFamily, String qualifier)
			throws IOException {
		HTable table = new HTable(configuration, tableName);
		Get g = new Get(rowKey.getBytes());
        Result r = table.get(g);
        byte[] bytecol = r.getValue(columnFamily.getBytes(), qualifier.getBytes());
        // 如果未没有找到指定值
        if (bytecol == null)
          bytecol =  new byte[0];
        String col= new String(bytecol);
		table.close();
		return col;
	}

	/**
	 * 通过rowkey的正则匹配数据 parma:tableName 表名 rowKeyRegex rowkey正则
	 */
	@Override
	public List<Result> getRecordByRowKeyRegex(String tableName,
			String rowKeyRegex) throws IOException {
		HTable table = new HTable(configuration, tableName);
		PrefixFilter filter = new PrefixFilter(rowKeyRegex.getBytes());
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		List<Result> list = new ArrayList<Result>();
		for (Result r : scanner) {
			list.add(r);
		}
		scanner.close();
		table.close();
		return list;
	}

	/**
	 * 通过rowkey的正则匹配数据 parma:tableName 表名 rowKeyRegex rowkey正则
	 */
	@Override
	public List<Result> getRecordByRowKeyRegex(String tableName,
			String rowKeyRegex, String[] cols) throws IOException {
		HTable table = new HTable(configuration, tableName);
		PrefixFilter filter = new PrefixFilter(rowKeyRegex.getBytes());
		Scan scan = new Scan();
		for (int i = 0; i < cols.length; i++) {
			scan.addColumn(Constant.COMMON_FAMILY.getBytes(), cols[i]
					.getBytes());
		}
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		List<Result> list = new ArrayList<Result>();
		for (Result r : scanner) {
			list.add(r);
		}
		scanner.close();
		table.close();
		return list;
	}
	
	/**
	 * 表内拷贝
	 * 
	 * @param tableName
	 * @param srcRowKeyPrefix
	 * @param dstRowKeyPrefix
	 * @throws IOException
	 */
	public void copy(String tableName, String srcRowKeyPrefix, 
			String dstRowKeyPrefix) throws IOException {
		
		HTable table = new HTable(configuration, tableName);
		PrefixFilter srcFilter = new PrefixFilter(srcRowKeyPrefix.getBytes());
		PrefixFilter dstFilter = new PrefixFilter(dstRowKeyPrefix.getBytes());
		Scan scan = new Scan();
		scan.setFilter(srcFilter);
		ResultScanner srcScanner = table.getScanner(scan);
		scan.setFilter(dstFilter);
		ResultScanner dstScanner = table.getScanner(scan);
		List<Delete> delList =  new ArrayList<Delete>();
		List<Put> putList = new ArrayList<Put>();
		for (Result r : dstScanner) {
			 delList.add(new Delete(r.getRow()));
		}
		table.delete(delList);
		
		for (Result r : srcScanner) {
			List<Cell> listCell = r.listCells();
			Put p = null;
			for (Cell c : listCell) {
				String row = new String(CellUtil.cloneRow(c)).replaceFirst(srcRowKeyPrefix, dstRowKeyPrefix);
				p = new Put(row.getBytes());
				p.add(CellUtil.cloneFamily(c), CellUtil.cloneQualifier(c), CellUtil.cloneValue(c));
				putList.add(p);
			}
		}
		table.put(putList);
		
		srcScanner.close();
		dstScanner.close();
		table.close();
	}
	
	/**
	 * 表间拷贝
	 * 
	 * @param srcTableName
	 * @param dstTableName
	 * @param srcRowKeyPrefix
	 * @param dstRowKeyPrefix
	 * @throws IOException
	 */
	public void copy(String srcTableName, String dstTableName, 
			String srcRowKeyPrefix, String dstRowKeyPrefix) throws IOException {
		
		HTable srcTable = new HTable(configuration, srcTableName);
		HTable dtsTable = new HTable(configuration, dstTableName);
		PrefixFilter srcFilter = new PrefixFilter(srcRowKeyPrefix.getBytes());
		PrefixFilter dstFilter = new PrefixFilter(dstRowKeyPrefix.getBytes());
		
		Scan scan = new Scan();
		scan.setFilter(srcFilter);
		ResultScanner srcScanner = srcTable.getScanner(scan);
		scan.setFilter(dstFilter);
		ResultScanner dstScanner = dtsTable.getScanner(scan);
		List<Delete> delList =  new ArrayList<Delete>();
		List<Put> putList = new ArrayList<Put>();
		for (Result r : dstScanner) {
			 delList.add(new Delete(r.getRow()));
		}
		dtsTable.delete(delList);
		
		for (Result r : srcScanner) {
			List<Cell> listCell = r.listCells();
			Put p = null;
			for (Cell c : listCell) {
				String row = new String(CellUtil.cloneRow(c)).replaceFirst(srcRowKeyPrefix, dstRowKeyPrefix);
				p = new Put(row.getBytes());
				p.add(CellUtil.cloneFamily(c), CellUtil.cloneQualifier(c), CellUtil.cloneValue(c));
				putList.add(p);
			}
		}
		dtsTable.put(putList);
		
		srcScanner.close();
		dstScanner.close();
		srcTable.close();
		dtsTable.close();
	}
	
	/**
	 * 
	 * @param tblName
	 * @param row
	 * @param family
	 * @param qualifier
	 * @param amount
	 * @return
	 * @throws IOException
	 */
	public long incColVal(String tblName, String row, String family, 
			String qualifier, long amount) throws IOException {
		
		HTable t  = new HTable(configuration, tblName);
		long res = t.incrementColumnValue(row.getBytes(), family.getBytes(),
				qualifier.getBytes(), amount);
		t.close();
		
		return res;
	}
	/**
	 * 批量插入数据
	 * 
	 * @param tableName
	 * @param rowkeyList
	 * @param columnFamily
	 * @param qualifierArr
	 * @param value
	 * @throws IOException
	 */
	public void batchInsert(String tableName, List<Cell> cells) throws IOException {
		HTable table = new HTable(configuration, tableName);
		
		List<Put> putList = new ArrayList<Put>();
		for (int i = 0; i < cells.size(); i++) {
			Put put = new Put(CellUtil.cloneRow(cells.get(i)));
			put.add(CellUtil.cloneFamily(cells.get(i)), CellUtil.cloneQualifier(cells.get(i)),
					CellUtil.cloneValue(cells.get(i)));
			
			putList.add(put);
		}
		table.put(putList);
		table.flushCommits() ;
		table.close();
	}
	
	/**
	 * 插入单条记录
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param columnFamily
	 * @param qualifier
	 * @param value
	 * @throws IOException
	 */
	public void insert(String tableName, String rowkey,
			String columnFamily, String qualifier, String value) throws IOException
	{
		HTable table = new HTable(configuration, tableName);
		Put put = new Put(rowkey.getBytes());
		put.add(columnFamily.getBytes(), qualifier.getBytes(), value.getBytes());
		table.put(put);
		table.close();
	}
	
	/**
	 * 获取具有相同前缀的行
	 * 
	 * @param tableName
	 * @param rowKeyRegex
	 * @return
	 * @throws IOException
	 */
    public List<Result> getRecordByPrefix(String tableName,
            String prefix) throws IOException {
        HTable table = new HTable(configuration, tableName);
        PrefixFilter filter = new PrefixFilter(prefix.getBytes());
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        List<Result> list = new ArrayList<Result>();
        for (Result r : scanner) {
            list.add(r);
        }
        scanner.close();
        table.close();
        return list;
    }
	

	public static void main(String[] args) {
		BaseDaoImpl impl;
		try {
			impl = new BaseDaoImpl();
			// List<String> rowkeyList = new ArrayList<String>();
			// rowkeyList.add("test1");
			// rowkeyList.add("test2");
			// impl.insertRowkeys(TableContants.TABLE_CUSTOMER_TMP, rowkeyList,
			// "cf", "end_user_id");
			String cust_num = null;
			Result result =impl.getOneRecord(Constant.TABLE_AREA_ACCUMULATIVE_RSLT, "20140603_1");
			for (KeyValue keyValue : result.raw()) {
				if ("cust_num".equals(new String(keyValue.getQualifier()))) {
					cust_num = new String(keyValue.getValue()) ;
					break;
				}
			}
			System.out.println(cust_num);
//			System.out.println(CommonUtil.transformHistoryData((impl.getRecordByRowKeyRegex(TableContants.TABLE_CUSTOMER_RESULT, "1051605", new String[]{"customer_num","xValue"}))));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
