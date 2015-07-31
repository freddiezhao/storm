package com.yihaodian.bi.view;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.yihaodian.bi.hbase.dao.impl.BaseDaoImpl;

public class Representor implements Representable {

    BaseDaoImpl                _dao;
    public static final String CF = "cf";
    public static final String QL = "value";

    public Representor(){
        try {
            _dao = new BaseDaoImpl();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 求具有某以特定前缀的rowkey的值的和
     * 
     * @param tblName
     * @param prefix
     * @return
     * @throws IOException
     */
    public long sum(String tbl, String prefix) throws IOException {
        List<Result> res = _dao.getRecordByPrefix(tbl, prefix);
        long amount = 0;
        for (Result r : res) {
            amount += Bytes.toLong(r.getValue(CF.getBytes(), QL.getBytes()));
        }

        return amount;
    }
    
    /**
     * 求具有某以特定前缀的rowkey的值的和
     * 
     * @param map
     * @param prefix
     * @return
     * @throws IOException
     */
    public long sum(Map<String, Long> map, String prefix) throws IOException {
        
        long amount = 0;
        for (Map.Entry<String, Long> e : map.entrySet()) {
            String key = e.getKey();
            if (key.startsWith(prefix)) {
                amount += e.getValue();
            }
        }

        return amount;
    }

    /**
     * 求rowkey在某一个特定范围内的所有对应值的和
     * 
     * @param tblName
     * @param startRow
     * @param endRow
     * @return
     * @throws IOException
     */
    public long sum(String tbl, String startRow, String endRow) throws IOException {
        List<Result> res = _dao.getRecordByRange(tbl, startRow, endRow);
        long amount = 0;
        for (Result r : res) {
            amount += Bytes.toLong(r.getValue(CF.getBytes(), QL.getBytes()));
        }

        return amount;
    }

    @Override
    public Map<String, Long> dot(String tbl, String key) {
        HashMap<String, Long> rt = new HashMap<String, Long>();
        try {
            rt.put(key, Long.parseLong(_dao.getColumnValue(tbl, key, CF, QL)));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rt;
    }

    @Override
    public Map<String, Long> curve(String tbl, String startRow, String endRow) {
        HashMap<String, Long> rt = new HashMap<String, Long>();
        try {
            List<Result> res = _dao.getRecordByRange(tbl, startRow, endRow);
            String preKey = null;
            String curKey = null;
            for (Result r : res) {
                curKey = new String(r.getRow());
                long val = Bytes.toLong(r.getValue(CF.getBytes(), QL.getBytes()));
                if (preKey != null) {
                    val += rt.get(preKey);
                }
                rt.put(curKey, val);
                preKey = curKey;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return rt;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Arguments missing");
            return;
        }
        String cmd = args[0].toLowerCase();
        String tbl = args[1].toLowerCase();

        if (cmd.equals("sum")) {
            if (args.length > 3) {
                String startRow = args[2];
                String endRow = args[3];
                System.out.println(tbl + " sum " + startRow + " to " + endRow + " : " +
                        new Representor().sum(tbl, startRow, endRow));
            }
            else if (args.length > 2) {
                String prefix = args[2];
                System.out.println(tbl + " sum " + prefix + "* : " +
                        new Representor().sum(tbl, prefix));
            }
        }
    }
}
