package com.yihaodian.bi.storm.common.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;

import com.yihaodian.bi.hbase.dao.BaseDao;
import com.yihaodian.bi.hbase.dao.impl.BaseDaoImpl;

public class DeleteAll {

	public static void main(String[] args) throws IOException {
		BaseDaoImpl dao = new BaseDaoImpl();
		String tableName = args[0];
		String rowKeyPrefix = args[1];
		
		dao.deleteAll(tableName, rowKeyPrefix);
//		List<Result> rsList = dao.getRecordByRowKeyRegex(tableName, rowKeyPrefix);
//        for (Result result : rsList) {
//            String rowKeyString = new String(result.getRow());
//            System.out.println("delete - " + rowKeyString);
//            dao.deleteRecord(tableName, rowKeyString);
//        }
        
        System.out.println("Successfully deleted all " + rowKeyPrefix + "* from " + tableName);

	}

}
