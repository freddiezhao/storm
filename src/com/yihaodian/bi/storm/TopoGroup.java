package com.yihaodian.bi.storm;

import java.lang.reflect.Method;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author zhaoheng
 */
public abstract class TopoGroup {

    private static final String BI_PREFiX = "BI_";

    private TopologyBuilder     tpBuilder = new TopologyBuilder();
    private String              gpName    = null;
    private String              realName  = null;

    private Config              conf      = new Config();

    public TopoGroup(String gpName){
        this.realName = gpName;
        this.gpName = BI_PREFiX + gpName;
    }

    public String getName() {
        return gpName;
    }

    public Config conf() {
        return conf;
    }

    public TopologyBuilder builder() {
        return tpBuilder;
    }

    public abstract void init(String[] args);

    public abstract String help();

    public void recovery(String[] args) {
        try {
            Class clazz = Class.forName("com.yihaodian.bi.storm.recovery." + realName);
            Object obj = clazz.newInstance();
            Method m = clazz.getMethod("recovery", String[].class);
            m.invoke(obj, (Object)args);
        } catch (ClassNotFoundException e) {
            System.out.println("com.yihaodian.bi.storm.recovery." + realName + " not found!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void monitor(String[] args) {
        try {
            Class clazz = Class.forName("com.yihaodian.bi.storm.monitor." + realName);
            Object obj = clazz.newInstance();
            Method m = clazz.getMethod("doMonitor", String[].class);
            m.invoke(obj, (Object)args);
        } catch (ClassNotFoundException e) {
            System.out.println("com.yihaodian.bi.storm.monitor." + realName + " not found!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
