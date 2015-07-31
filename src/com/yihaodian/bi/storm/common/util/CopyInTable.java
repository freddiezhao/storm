package com.yihaodian.bi.storm.common.util;

import java.io.IOException;

import com.yihaodian.bi.hbase.dao.impl.BaseDaoImpl;


/**
 * 表内拷贝数据
 * 
 * @author 
 *
 */
public class CopyInTable {

	public static void main(String[] args) throws IOException {
		BaseDaoImpl dao = new BaseDaoImpl();
		String tableName = args[0];
		String srcRowKeyPrefix = args[1];
		String dstRowKeyPrefix = args[2];
		
		dao.copy(tableName, srcRowKeyPrefix, dstRowKeyPrefix);
		
	}

}
