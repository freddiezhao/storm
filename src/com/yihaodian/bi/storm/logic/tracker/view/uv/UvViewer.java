package com.yihaodian.bi.storm.logic.tracker.view.uv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.yihaodian.bi.hbase.HBaseConstant;
import com.yihaodian.bi.view.Representor;

public class UvViewer extends Representor {

    /**
     * 绘制某一之间点之前的，某平台的有效Uv （H5，PC包括非有效Uv）
     * 
     * @param timeStamp 时间戳 i.e. 201507301200
     * @param pltmId 平台Id
     * @param login 登陆标志，1为登陆，0为未登陆
     * @return <k,v>对组成的结果集
     */
    public Map<String, Long> userCurve(String timeStamp, String pltmId, String login) {
        String win = timeStamp.substring(0, 8);
        String startRow = "user_" + pltmId + "_" + login + win;
        String endRow = "user_" + pltmId + "_" + login + "_" + timeStamp;

        return super.curve(HBaseConstant.Table.DEFAULT_KV_TABLE.tbName, startRow, endRow);
    }

    /**
     * 绘制某一之间点之前的，某省份的有效Uv
     * 
     * @param timeStamp 时间戳 i.e. 201507301200
     * @param provId 省份Id
     * @return <k,v>对组成的结果集
     */
    public Map<String, Long> provCurve(String timeStamp, String provId) {
        String win = timeStamp.substring(0, 8);
        String startRow = "prov_" + provId + "_" + win;
        String endRow = "prov_" + provId + "_" + timeStamp;

        return super.curve(HBaseConstant.Table.DEFAULT_KV_TABLE.tbName, startRow, endRow);
    }

    /**
     * 绘制某一之间点之前的，某渠道的有效Uv
     * 
     * @param timeStamp 时间戳 i.e. 201507301200
     * @param provId 渠道Id
     * @return 由<k,v>对组成的结果集
     */
    public Map<String, Long> chnlCurve(String timeStamp, String chnlId) {
        String win = timeStamp.substring(0, 8);
        String startRow = "chnl_" + chnlId + "_" + win;
        String endRow = "chnl_" + chnlId + "_" + timeStamp;

        return super.curve(HBaseConstant.Table.DEFAULT_KV_TABLE.tbName, startRow, endRow);
    }

    /**
     * 绘制某一时间点之前的全站有效Uv
     * 
     * @param timeStamp 时间戳 i.e. 201507301200
     * @return 由<k,v>对组成的结果集
     */
    public Map<String, Long> globCurve(String timeStamp) {
        String win = timeStamp.substring(0, 8);
        String startRow = "glob_" + win;
        String endRow = "glob_" + timeStamp;

        return super.curve(HBaseConstant.Table.DEFAULT_KV_TABLE.tbName, startRow, endRow);
    }
    
    /**
     * 功能函数 用于测试
     * 
     * @param map
     */
    private static void traverseMap(Map<String, ?> map) {
        List<String> kvList = new ArrayList<String>();
        for (Map.Entry<?, ?> e : map.entrySet()) {
            kvList.add(e.getKey() + "=>" + e.getValue());
        }
        String[] kvArray = kvList.toArray(new String[0]);
        Arrays.sort(kvArray);
        for (int i = 0; i < kvArray.length; i++) {
            System.out.println(kvArray[i]);
        }
    }

    public static void main(String[] args) {
        UvViewer viewer = new UvViewer();

        Map<String, Long> userMap = viewer.userCurve("201507301210", "1", "1");
        Map<String, Long> provMap = viewer.provCurve("201507301210", "1");
        Map<String, Long> chnlMap = viewer.chnlCurve("201507301210", "1");
        Map<String, Long> globMap = viewer.globCurve("201507301210");

        UvViewer.traverseMap(userMap);
        UvViewer.traverseMap(provMap);
        UvViewer.traverseMap(chnlMap);
        UvViewer.traverseMap(globMap);

    }

}
