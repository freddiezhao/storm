package com.yihaodian.bi.storm.common.util;

import java.io.IOException;

import com.yihaodian.bi.hbase.dao.impl.BaseDaoImpl;

public class CopyOverTable {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		BaseDaoImpl dao = new BaseDaoImpl();
		
		dao.copy(args[0], args[1], args[2], args[3]);
		
	}

}
