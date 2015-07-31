package com.yihaodian.bi.storm.recovery;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.impl.BaseDaoImpl;
import com.yihaodian.bi.storm.common.util.CommonUtil;
import com.yihaodian.bi.storm.common.util.Constant;

public class MobileSales extends BaseDaoImpl {

    private String SQL_RECOVER_DATA_LINE;
    private String SQL_RECOVER_DATA_COL;

	public MobileSales() throws IOException {
		super();
	}
    public MobileSales(String date) throws IOException{
        this.date = date;
        SQL_RECOVER_DATA_LINE	=" SELECT to_char(date_time_id, 'yyyymmdd hh24mi') AS date_id,\r\n" + 
		"       SUM(a.ordr_amt) AS ordr_amt\r\n" + 
		"FROM   (SELECT /*+index(t4) use_nl(t1 t2) parallel(8)*/\r\n" + 
		"         CASE\r\n" + 
		"            WHEN t9.payment_category = 1 THEN\r\n" + 
		"             t1.order_payment_confirm_date\r\n" + 
		"            ELSE\r\n" + 
		"             t1.order_create_time\r\n" + 
		"         END date_time_id,\r\n" + 
		"         \r\n" + 
		"         t2.order_item_amount - t2.promotion_amount - t2.coupon_amount -\r\n" + 
		"         ratio_to_report(t2.order_item_amount) OVER(PARTITION BY t2.order_id) * t1.order_paid_by_rebate AS ordr_amt\r\n" + 
		"        FROM   stage_user.so_real t1\r\n" + 
		"        INNER  JOIN stage_user.so_item_real t2\r\n" + 
		"        ON     t1.id = t2.order_id\r\n" + 
		"        INNER  JOIN dw.dim_mrchnt t3\r\n" + 
		"        ON     t2.merchant_id = t3.mrchnt_id\r\n" + 
		"        AND    t3.cur_flag = 1\r\n" + 
		"        INNER  JOIN dw.dim_prod t4\r\n" + 
		"        ON     t2.product_id = t4.prod_id\r\n" + 
		"        AND    t4.cur_flag = 1\r\n" + 
		"        INNER  JOIN dw.dim_brand t5\r\n" + 
		"        ON     t4.brand_id = t5.brand_id\r\n" + 
		"        AND    t5.cur_flag = 1\r\n" + 
		"        INNER  JOIN dw.hier_categ t6\r\n" + 
		"        ON     t4.categ_lvl_id = t6.categ_lvl_id\r\n" + 
		"        AND    t6.cur_flag = 1\r\n" + 
		"        INNER  JOIN dw.dim_payment_type t9\r\n" + 
		"        ON     t1.pay_service_type = t9.id\r\n" + 
		"        WHERE  t4.prod_type NOT IN (4, 7) -- 剔除礼品卡\r\n" + 
		"        AND    (t2.order_item_amount <> 0 OR\r\n" + 
		"              (t2.order_item_amount = 0 AND t2.total_integral <> 0)) -- 剔除0元非积分兑换商品\r\n" + 
		"        AND    t1.is_leaf = 1\r\n" + 
		"        AND    t2.is_item_leaf = 1\r\n" + 
		"        AND    ((t9.payment_category = 1 --网上支付\r\n" + 
		"              AND t1.order_payment_confirm_date >=\r\n" + 
		"              trunc(to_date('"+date+"', 'yyyyMMdd')) AND\r\n" + 
		"              t1.order_payment_confirm_date <\r\n" + 
		"              trunc(to_date('"+date+"', 'yyyyMMdd') + 1) AND\r\n" + 
		"              NOT (t1.cancel_date IS NOT NULL AND\r\n" + 
		"               trunc(t1.order_payment_confirm_date) = trunc(t1.cancel_date))) OR\r\n" + 
		"              (t9.payment_category = 2 --货到付款\r\n" + 
		"              AND t1.order_create_time >= trunc(to_date('"+date+"', 'yyyyMMdd')) AND\r\n" + 
		"              t1.order_create_time < trunc(to_date('"+date+"', 'yyyyMMdd') + 1) AND\r\n" + 
		"              NOT (t1.cancel_date IS NOT NULL AND\r\n" + 
		"               trunc(t1.order_create_time) = trunc(t1.cancel_date))))) a\r\n" + 
		"\r\n" + 
		"GROUP  BY to_char(date_time_id, 'yyyymmdd hh24mi')\r\n" + 
		"ORDER  BY to_char(date_time_id, 'yyyymmdd hh24mi')\r\n" + 
		"";


	SQL_RECOVER_DATA_COL=" SELECT to_char(date_time_id, 'hh24')+1  AS hour,\r\n" + 
			"       SUM(a.ordr_amt) AS ordr_amt\r\n" + 
			"FROM   (SELECT /*+index(t4) use_nl(t1 t2) parallel(8)*/\r\n" + 
			"         CASE\r\n" + 
			"            WHEN t9.payment_category = 1 THEN\r\n" + 
			"             t1.order_payment_confirm_date\r\n" + 
			"            ELSE\r\n" + 
			"             t1.order_create_time\r\n" + 
			"         END date_time_id,\r\n" + 
			"         \r\n" + 
			"         t2.order_item_amount - t2.promotion_amount - t2.coupon_amount -\r\n" + 
			"         ratio_to_report(t2.order_item_amount) OVER(PARTITION BY t2.order_id) * t1.order_paid_by_rebate AS ordr_amt\r\n" + 
			"        FROM   stage_user.so_real t1\r\n" + 
			"        INNER  JOIN stage_user.so_item_real t2\r\n" + 
			"        ON     t1.id = t2.order_id\r\n" + 
			"        INNER  JOIN dw.dim_mrchnt t3\r\n" + 
			"        ON     t2.merchant_id = t3.mrchnt_id\r\n" + 
			"        AND    t3.cur_flag = 1\r\n" + 
			"        INNER  JOIN dw.dim_prod t4\r\n" + 
			"        ON     t2.product_id = t4.prod_id\r\n" + 
			"        AND    t4.cur_flag = 1\r\n" + 
			"        INNER  JOIN dw.dim_brand t5\r\n" + 
			"        ON     t4.brand_id = t5.brand_id\r\n" + 
			"        AND    t5.cur_flag = 1\r\n" + 
			"        INNER  JOIN dw.hier_categ t6\r\n" + 
			"        ON     t4.categ_lvl_id = t6.categ_lvl_id\r\n" + 
			"        AND    t6.cur_flag = 1\r\n" + 
			"        INNER  JOIN dw.dim_payment_type t9\r\n" + 
			"        ON     t1.pay_service_type = t9.id\r\n" + 
			"        WHERE  t4.prod_type NOT IN (4, 7) -- 剔除礼品卡\r\n" + 
			"        AND    (t2.order_item_amount <> 0 OR\r\n" + 
			"              (t2.order_item_amount = 0 AND t2.total_integral <> 0)) -- 剔除0元非积分兑换商品\r\n" + 
			"        AND    t1.is_leaf = 1\r\n" + 
			"        AND    t2.is_item_leaf = 1\r\n" + 
			"        AND    ((t9.payment_category = 1 --网上支付\r\n" + 
			"              AND t1.order_payment_confirm_date >=\r\n" + 
			"              trunc(to_date('"+date+"', 'yyyyMMdd')) AND\r\n" + 
			"              t1.order_payment_confirm_date <\r\n" + 
			"              trunc(to_date('"+date+"', 'yyyyMMdd') + 1) AND\r\n" + 
			"              NOT (t1.cancel_date IS NOT NULL AND\r\n" + 
			"               trunc(t1.order_payment_confirm_date) = trunc(t1.cancel_date))) OR\r\n" + 
			"              (t9.payment_category = 2 --货到付款\r\n" + 
			"              AND t1.order_create_time >= trunc(to_date('"+date+"', 'yyyyMMdd')) AND\r\n" + 
			"              t1.order_create_time < trunc(to_date('"+date+"', 'yyyyMMdd') + 1) AND\r\n" + 
			"              NOT (t1.cancel_date IS NOT NULL AND\r\n" + 
			"               trunc(t1.order_create_time) = trunc(t1.cancel_date))))) a\r\n" + 
			"\r\n" + 
			"GROUP  BY to_char(date_time_id, 'hh24')\r\n" + 
			"order by to_char(date_time_id, 'hh24')\r\n" + 
			"";
    
    }

    private String            date;

    private Connection        conn = null;
    private PreparedStatement st   = null;
    private ResultSet         rs   = null;

    private String            tableName;
    private String            rowKeyRegexLine; // 线图rowkey
    private String            rowKeyRegexCol;  // 柱状图rowkey

    /**
     * 恢复移动订单净额
     * 
     * @param date 操作日期 20140620
     */
    public boolean startRecoverData() {
        tableName = Constant.TABLE_MOBILE_ORDER_INFO_RESULT;
        /** 线图 */
        rowKeyRegexLine = date + "_amount_";
        recoverLineData(tableName, rowKeyRegexLine);
        System.out.println("移动订单净额线图数据修复完成！");

        /** 柱状图 */
        rowKeyRegexCol = "col_data_" + date;
        recoverColData(tableName, rowKeyRegexCol);
        System.out.println("移动订单净额柱状图数据修复完成！");
        return true;
    }

    /**
     * 恢复移动订单净额线图
     * 
     * @param tableName
     * @param rowKeyRegexLine
     */
    private void recoverLineData(String tableName, String rowKeyRegexLine) {
        // 首先需要干掉错误数据
        List<Result> listHis;
        try {
            listHis = super.getRecordByRowKeyRegex(tableName, rowKeyRegexLine);

            for (Result r : listHis) {
                super.deleteRecord(tableName, new String(r.getRow()));
            }
            // 查询oracle数据并写入hbase
            List<RecoveryDataVo> list = this.getRecoveryLineData(rowKeyRegexLine);
            insertRecord(tableName, Constant.COMMON_FAMILY, list);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量插入 否则会报内存溢出 怀疑是多次new HTable导致
     * 
     * @param tableName
     * @param columnFamily
     * @param list
     * @throws IOException
     */
    public void insertRecord(String tableName, String columnFamily, List<RecoveryDataVo> list) throws IOException {
        HTable table = new HTable(configuration, tableName);
        List<Put> putList = new ArrayList<Put>();
        for (RecoveryDataVo rdv : list) {
            String[] valueArr = new String[] { rdv.getColumn1(), rdv.getXTitle(), rdv.getXValue() };
            String[] qualifierArr = new String[] { "order_amount", "xTitle", "xValue" };
            Put put = new Put(rdv.getRowkey().getBytes());
            for (int i = 0; i < qualifierArr.length; i++) {
                String col = qualifierArr[i];
                String val = valueArr[i];
                put.add(columnFamily.getBytes(), col.getBytes(), val.getBytes());
            }
            putList.add(put);

        }
        table.put(putList);
        table.flushCommits();
        table.close();
        daoMsg = "插入行成功";
    }

    /**
     * 恢复移动订单净额柱状图
     * 
     * @param tableName
     * @param rowKeyRegexLine
     */
    private void recoverColData(String tableName, String rowKeyRegexLine) {
        // 首先需要干掉错误数据
        List<Result> listHis;
        try {
            listHis = super.getRecordByRowKeyRegex(tableName, rowKeyRegexLine);

            for (Result r : listHis) {
                super.deleteRecord(tableName, new String(r.getRow()));
            }
            // 查询oracle数据并写入hbase
            List<RecoveryDataVo> list = this.getRecoveryColData();
            String colValue = "[";
            // 拼接小时字符串
            for (RecoveryDataVo rdv : list) {
                colValue += "[" + rdv.getColumn1() + "," + rdv.getColumn2() + "],";
            }
            colValue = colValue.substring(0, colValue.length() - 1);
            colValue += "]";
            System.out.println("colValue==" + colValue);
            super.insertRecord(tableName, rowKeyRegexLine, Constant.COMMON_FAMILY, "col_data", colValue);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 查询全站订单净额和移动订单净额 线图数据
     * 
     * @param rowKeyRegex
     * @return
     */
    public List<RecoveryDataVo> getRecoveryLineData(String rowKeyRegex) {
        OracleConnection oc = new OracleConnection();
        List<RecoveryDataVo> list = new LinkedList<RecoveryDataVo>();
        RecoveryDataVo rdv = null;
        try {
            // 查询当天信息
            conn = oc.getConnection();
            st = conn.prepareStatement(SQL_RECOVER_DATA_LINE, ResultSet.TYPE_SCROLL_INSENSITIVE,
                                       ResultSet.CONCUR_UPDATABLE);
            System.out.println("修复移动线图==" + SQL_RECOVER_DATA_LINE);
            rs = st.executeQuery();
            while (rs.next()) {
                String[] strLastXValue = null;
                String date_id = rs.getString(1);// ep 20140620 0001
                String ordr_amt = rs.getString(2);// ep 25286.84
                String[] rowkeyArr = date_id.split(" ");
                String date = rowkeyArr[0];
                String time = rowkeyArr[1];
                String keyTag = date + time;
                String rowkey = rowKeyRegex + "1" + keyTag;
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
                Date tmpd = sdf.parse(keyTag);
                strLastXValue = CommonUtil.getCurFullXValueByOrdrTime(tmpd);
                String xTitle = strLastXValue[0];
                String xValue = strLastXValue[1];

                rdv = new RecoveryDataVo();
                rdv.setRowkey(rowkey);
                rdv.setColumn1(ordr_amt);
                rdv.setXTitle(xTitle);
                rdv.setXValue(xValue);
                list.add(rdv);
            }
            if (rs.last()) {
                String[] strLastXValue = null;
                String date_id = rs.getString(1);// ep 20140620 0001
                String ordr_amt = rs.getString(2);// ep 25286.84
                String[] rowkeyArr = date_id.split(" ");
                String date = rowkeyArr[0];
                String time = rowkeyArr[1];
                String keyTag = date + time;
                String rowkey = "last_amount_" + date;
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
                Date tmpd = sdf.parse(keyTag);
                strLastXValue = CommonUtil.getCurFullXValueByOrdrTime(tmpd);
                String xTitle = strLastXValue[0];
                String xValue = strLastXValue[1];

                rdv = new RecoveryDataVo();
                rdv.setRowkey(rowkey);
                rdv.setColumn1(ordr_amt);
                rdv.setXTitle(xTitle);
                rdv.setXValue(xValue);
                list.add(rdv);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            oc.closeConnection(rs, st, conn);
        }
        return list;
    }

    /**
     * 查询移动订单净额 线图数据
     * 
     * @param rowKeyRegex
     * @return
     */
    public List<RecoveryDataVo> getRecoveryColData() {
        OracleConnection oc = new OracleConnection();
        List<RecoveryDataVo> list = new LinkedList<RecoveryDataVo>();
        RecoveryDataVo rdv = null;
        try {
            // 查询当天信息
            conn = oc.getConnection();
            st = conn.prepareStatement(SQL_RECOVER_DATA_COL);
            System.out.println("修复移动柱状图==" + SQL_RECOVER_DATA_COL);
            rs = st.executeQuery();
            while (rs.next()) {
                rdv = new RecoveryDataVo();
                rdv.setColumn1(rs.getString(1));
                rdv.setColumn2(rs.getString(2));
                list.add(rdv);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            oc.closeConnection(rs, st, conn);
        }
        return list;
    }

    public static void recovery(String args[]) {
        if (args == null || args.length < 3 || StringUtils.isEmpty(args[0])) {
            System.out.println("Missing params：date or recovery or gpName");
        }
        final String date = args[2]; // "20150210"
        if (args.length == 3) {
            try {
                new MobileSales(date).startRecoverData();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Missing date params");
        }
    }

}
