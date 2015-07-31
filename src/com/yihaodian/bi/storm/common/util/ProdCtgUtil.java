package com.yihaodian.bi.storm.common.util;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import com.yihaodian.bi.hbase.dao.BaseDao;
import com.yihaodian.bi.storm.common.model.CtgVO;

public class ProdCtgUtil {

	/**
	 * @param args
	 */

	public static Map<String, String> getProdCtgMap() {
		Map<String, String> prodCtgMap = new HashMap<String, String>();
		prodCtgMap.put("-19", "汽车生活");
		prodCtgMap.put("-18", "生活电器");
		prodCtgMap.put("-17", "饮料");
		prodCtgMap.put("-16", "食品");
		prodCtgMap.put("-14", "数码");
		prodCtgMap.put("-13", "手机");
		prodCtgMap.put("5009", "美容护理");
		prodCtgMap.put("5117", "母婴");
		prodCtgMap.put("5134", "厨卫清洁");
		prodCtgMap.put("8644", "进口食品");
		prodCtgMap.put("8704", "营养保健");
		prodCtgMap.put("950340", "家居");
		prodCtgMap.put("956955", "生鲜食品");
		prodCtgMap.put("957427", "电脑办公");
		// FASHION
		prodCtgMap.put("971277", "运动户外");
		prodCtgMap.put("969355", "珠宝饰品");
		prodCtgMap.put("-20", "鞋靴");
		prodCtgMap.put("-21", "服装");
		prodCtgMap.put("959325", "箱包");

		return prodCtgMap;
	}

	public static Map<String, String> getCtgDeptMap() {
		Map<String, String> prodCtgMap = new HashMap<String, String>();
		prodCtgMap.put("-19", "CE");
		prodCtgMap.put("-18", "CE");
		prodCtgMap.put("-17", "FOODS");
		prodCtgMap.put("-16", "FOODS");
		prodCtgMap.put("-14", "CE");
		prodCtgMap.put("-13", "CE");
		prodCtgMap.put("5009", "NON-FOODS");
		prodCtgMap.put("5117", "NON-FOODS");
		prodCtgMap.put("5134", "NON-FOODS");
		prodCtgMap.put("8644", "FOODS");
		prodCtgMap.put("8704", "FOODS");
		prodCtgMap.put("950340", "NON-FOODS");
		prodCtgMap.put("956955", "FOODS");
		prodCtgMap.put("957427", "CE");

		// FASHION
		prodCtgMap.put("971277", "FASHION");
		prodCtgMap.put("969355", "FASHION");
		prodCtgMap.put("-20", "FASHION");
		prodCtgMap.put("-21", "FASHION");
		prodCtgMap.put("959325", "FASHION");
		return prodCtgMap;
	}

	public static List<String> getCtgIDList() {
		List<String> l = new ArrayList<String>();
		Iterator<String> iterator = getProdCtgMap().keySet().iterator();
		while (iterator.hasNext()) {
			String code = iterator.next();
			if (!"status".equals(code)) {
				l.add(code);
			}
		}
		return l;
	}

	public static List<CtgVO> getData(BaseDao dao, String rowKeyRegex, int type) {
		List<CtgVO> list = new ArrayList<CtgVO>();
		List<Result> rsList = null;
		try {
			rsList = dao.getRecordByRowKeyRegex(Constant.TABLE_CTG_RSLT_NEW,
					rowKeyRegex);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (Result result : rsList) {
			// 一个result里有多列，取需要的列
			CtgVO vo = new CtgVO();
			String rowString = new String(result.getRow());
			if (rowString.split("_").length < 2) {
				continue;
			}
			String ctgId = "";
			if(1==type){
				ctgId = rowString.split("_")[2];
			}else{
				ctgId = rowString.split("_")[1];
			}
//			System.out.println("ctgId=="+ctgId);
			if (!ctgId.equals("null")) {
				vo.setCtgId(ctgId);
				vo.setDeptId(ProdCtgUtil.getCtgDeptMap().get(ctgId));
				vo.setCtgCnName(ProdCtgUtil.getProdCtgMap().get(ctgId));
				for (KeyValue keyValue : result.raw()) {
					if ("customer_num".equals(new String(keyValue
							.getQualifier()))) {
						vo.setCustomer_num(new String(keyValue.getValue()));
					}
					if ("order_amount".equals(new String(keyValue
							.getQualifier()))) {
						vo.setOrder_amount(new String(keyValue.getValue()));
					}
					if ("order_num".equals(new String(keyValue.getQualifier()))) {
						vo.setOrder_num(new String(keyValue.getValue()));
					}
					if ("sku_num".equals(new String(keyValue.getQualifier()))) {
						vo.setSku_num(new String(keyValue.getValue()));
					}
				}
			}
			list.add(vo);

		}
		return list;
	}

	/**
	 * @param categ
	 *            1 CE 2FOODS 3NON-FOODS
	 * @param rowKeyRegex
	 * @param target
	 *            指标 1顾客数 2订单净额 3订单数 4sku
	 * @return
	 */
	public static String[] getDataByCateg(BaseDao dao, String categ,
			String rowKeyRegex, int target) {
		List<CtgVO> list = getData(dao, rowKeyRegex, 2);
		String[] backArr = new String[] { "\'[]\'", "\'[]\'" };
		String backStr = "\'[";
		String backNameStr = "\'[";
		for (CtgVO vo : list) {
			/** 类目判断 */
			if (categ.equals(vo.getDeptId())) {
				/** 指标判断 */
				if (target == 1) {// 顾客数
					backStr += vo.getCustomer_num() + ",";
				} else if (target == 2) {// 订单净额
					backStr += getFmtNoPoint(vo.getOrder_amount()) + ",";
				} else if (target == 3) {// 订单数
					backStr += vo.getOrder_num() + ",";
				} else if (target == 4) {// sku
					backStr += vo.getSku_num() + ",";
				}
				backNameStr += vo.getCtgCnName() + ",";
			}
		}
		if (backStr.indexOf(",") != -1) {
			backStr = backStr.substring(0, backStr.length() - 1) + "]\'";
		} else {
			backStr = "\'[]\'";
		}
		if (backNameStr.indexOf(",") != -1) {
			backNameStr = backNameStr.substring(0, backNameStr.length() - 1)
					+ "]\'";
		} else {
			backNameStr = "\'[]\'";
		}
		backArr[0] = backStr;// 数据
		backArr[1] = backNameStr;// 类目名
		return backArr;
	}

	/**
	 * @param rowKeyRegex
	 * @param target
	 * @param type 1当天 2 7天前
	 *            指标 1顾客数 2订单净额 3订单数 4sku
	 * @return
	 */
	public static String[] getDataAllCateg(BaseDao dao, String rowKeyRegex,
			int target, int type) {
		List<CtgVO> list = getData(dao, rowKeyRegex, type);
		String[] backArr = new String[] { "\'[]\'", "\'[]\'" };
		String backStr = "\'[";
		String backNameStr = "\'[";

		String ceStr = "";
		String foodStr = "";
		String nonFoodStr = "";
		String fashionStr = "";
		String ceName = "";
		String foodName = "";
		String nonFoodName = "";
		String fashionName = "";
		for (CtgVO vo : list) {
			if ("FOODS".equals(vo.getDeptId())) {
				/** 指标判断 */
				if (target == 1) {// 顾客数
					foodStr += vo.getCustomer_num() + ",";
				} else if (target == 2) {// 订单净额
					foodStr += getFmtNoPoint(vo.getOrder_amount()) + ",";
				} else if (target == 3) {// 订单数
					foodStr += vo.getOrder_num() + ",";
				} else if (target == 4) {// sku
					foodStr += vo.getSku_num() + ",";
				}
				foodName += vo.getCtgCnName() + ",";
			}
			if ("NON-FOODS".equals(vo.getDeptId())) {
				/** 指标判断 */
				if (target == 1) {// 顾客数
					nonFoodStr += vo.getCustomer_num() + ",";
				} else if (target == 2) {// 订单净额
					nonFoodStr += getFmtNoPoint(vo.getOrder_amount()) + ",";
				} else if (target == 3) {// 订单数
					nonFoodStr += vo.getOrder_num() + ",";
				} else if (target == 4) {// sku
					nonFoodStr += vo.getSku_num() + ",";
				}
				nonFoodName += vo.getCtgCnName() + ",";
			}
			if ("CE".equals(vo.getDeptId())) {
				/** 指标判断 */
				if (target == 1) {// 顾客数
					ceStr += vo.getCustomer_num() + ",";
				} else if (target == 2) {// 订单净额
					ceStr += getFmtNoPoint(vo.getOrder_amount()) + ",";
				} else if (target == 3) {// 订单数
					ceStr += vo.getOrder_num() + ",";
				} else if (target == 4) {// sku
					ceStr += vo.getSku_num() + ",";
				}
				ceName += vo.getCtgCnName() + ",";
			}
			if ("FASHION".equals(vo.getDeptId())) {
				/** 指标判断 */
				if (target == 1) {// 顾客数
					fashionStr += vo.getCustomer_num() + ",";
				} else if (target == 2) {// 订单净额
					fashionStr += getFmtNoPoint(vo.getOrder_amount()) + ",";
				} else if (target == 3) {// 订单数
					fashionStr += vo.getOrder_num() + ",";
				} else if (target == 4) {// sku
					fashionStr += vo.getSku_num() + ",";
				}
				fashionName += vo.getCtgCnName() + ",";
			}
		}
		backStr = backStr + foodStr + nonFoodStr + ceStr + fashionStr;
		backNameStr = backNameStr + foodName + nonFoodName + ceName
				+ fashionName;
		if (backStr.indexOf(",") != -1) {
			backStr = backStr.substring(0, backStr.length() - 1) + "]\'";
		} else {
			backStr = "\'[]\'";
		}
		if (backNameStr.indexOf(",") != -1) {
			backNameStr = backNameStr.substring(0, backNameStr.length() - 1)
					+ "]\'";
		} else {
			backNameStr = "\'[]\'";
		}
		backArr[0] = backStr;// 数据
		backArr[1] = backNameStr;// 类目名
		return backArr;
	}

	public static String getFmtNoPoint(String str) {
		DecimalFormat format_noPoint = new DecimalFormat("#");
		if (str != null) {
			return format_noPoint.format(Double.parseDouble(str));
		}
		return "0";
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// System.out.println(ProdCtgUtil.initProdCtgMap().keySet().size());
	}

}
