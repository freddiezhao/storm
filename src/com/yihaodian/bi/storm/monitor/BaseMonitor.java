package com.yihaodian.bi.storm.monitor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;

import com.yihaodian.bi.common.util.DateUtil;
import com.yihaodian.bi.common.util.NetUtil;
import com.yihaodian.bi.database.impl.OracleConnection;
import com.yihaodian.bi.hbase.dao.GosOrderDao;
import com.yihaodian.bi.hbase.dao.impl.GosOrderDaoImpl;
import com.yihaodian.bi.hbase.dao.impl.TrackerDaoImpl;

public abstract class BaseMonitor {

    protected static final String           TOPIC      = "STORM监控";
    protected static Map<String, String>    CACHED     = new HashMap<String, String>();
    protected static final Logger           LOG        = LoggerFactory.getLogger(BaseMonitor.class);
    protected static final OracleConnection CONNECTOR  = new OracleConnection();
    
    protected Connection                    oracleCon;
    protected HBaseInitThread               hbaseDao   = new HBaseInitThread();
    public boolean                          debug      = false;

    public static String                    CALLNUMBER = "13554106450";

    public static class HBaseConnectionException extends Exception {

        public HBaseConnectionException(){
            super("HBase Connection Exception!");
        }
    }

    public static class OracleConnectionException extends Exception {

        public OracleConnectionException(){
            super("Oracle Connection Exception!");
        }
    }

    public static class HBaseInitThread extends Thread {

        protected BaseMonitor    base;
        protected GosOrderDao    hbaseOrderDao   = null;
        protected TrackerDaoImpl hbaseTrackerDao = null;
        protected boolean        initOk          = false;

        @Override
        public void run() {
            try {
                msg("Thread.ID[" + Thread.currentThread().getId() + "] Init HBase connection");
                hbaseOrderDao = new GosOrderDaoImpl();
                initOk = true;
            } catch (IOException e) {
            }
        }

        public GosOrderDao getOrderDao() {
            if (hbaseOrderDao == null && !base.debug) {
                int x = 0;
                while (!initOk) {
                    try {
                        msg("Thread.ID[" + Thread.currentThread().getId() + "]Waiting for HBase connect .... 1000ms");
                        Thread.sleep(3000);
                        x++;
                    } catch (Exception e) {
                    }
                    if (x == 3) {
                        throw new RuntimeException("Thread.ID[" + Thread.currentThread().getId()
                                                   + "] Can not connect to Hbase Server! ");
                    }
                }
            }
            return hbaseOrderDao;
        }
    };

    /**
     */
    protected abstract void monitor(String[] args) throws Exception;

    public void init() {
        try {
            oracleCon = CONNECTOR.getConnection();
            hbaseDao.base = this;
            hbaseDao.setDaemon(true);
            hbaseDao.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HBaseInitThread getHBase() {
        return hbaseDao;
    }

    public static void msg(String msg) {
        System.out.println(msg);
    }

    public abstract void help();

    public void doMonitor(String[] args) throws Exception {
        msg("\n\n ========= - START - " + getMonitorName() + " ==========");
        if (args.length <= 0 || (args.length > 0 && args[0].toLowerCase().indexOf("-help") >= 0)) {
            help();
            msg("单独调用方式： java -cp ${STORM.Jar包名称} " + this.getClass().getPackage().getName() + "."
                + this.getClass().getName() + " 参数列表");
            msg("========= - END - " + getMonitorName() + " ========== \n\n ");
            return;
        }
        CALLNUMBER = args[0];
        String[] argCp = (String[]) Arrays.copyOfRange(args, 1, args.length);
        init();
        initCacheFile();
        Exception exe = null;
        try{
        	monitor(argCp);
        }catch(Exception e){
        	exe = e;
        }
        writeCacheFile();
        if(exe != null)
        	throw exe;
        if (hbaseDao.isAlive()) {
            hbaseDao.stop();
        }
        msg("========= - END - " + getMonitorName() + " ========== \n\n ");
    }

    private static String getCacheFileStr() {
        String cfStr = null;
        if (!SystemUtils.IS_OS_LINUX) {
            cfStr = "./" + DateUtil.YYYY_MM_DD.format(new Date());
        }
        if (cfStr == null) cfStr = "/tmp/storm_monitor." + DateUtil.YYYY_MM_DD.format(new Date());
        return cfStr;
    }

    private static void initCacheFile() {
        String cfStr = getCacheFileStr();
        try {
            File cf = new File(cfStr);
            if (cf.exists()) {
                FileInputStream fs = new FileInputStream(cf);
                ObjectInputStream os = new ObjectInputStream(fs);
                Object obj = os.readObject();
                if (obj != null) {
                    msg("cache init ok " + cfStr);
                    CACHED = (HashMap<String, String>) obj;
                } else {
                    msg("cache init error ! " + cfStr);
                }
            }
        } catch (Exception e) {
            msg(e.getMessage());
            LOG.error("initcache file error! " + cfStr, e);
        }
    }

    private static boolean writeCacheFile() {
        String cfStr = getCacheFileStr();
        try {
            File cf = new File(cfStr);
            FileOutputStream fs = new FileOutputStream(cf);
            ObjectOutputStream os = new ObjectOutputStream(fs);
            os.writeObject(CACHED);
            os.flush();
            os.close();
            fs.close();
            msg("cache write ok " + cfStr);
            return true;
        } catch (Exception e) {
            msg("write cache error!");
            LOG.error("write cache error!", e);
            return false;
        }
    }

    public abstract String getMonitorName();

    public void sendEmailByCache(String msg, String topic) throws Exception {

        if (CACHED.get(msg + "_mail") == null) {
            CACHED.put(msg + "_mail", "TRUE");
            if (topic == null) {
                topic = TOPIC;
            }
            NetUtil.sendEmailBycurlD("BI_Operation@yhd.com", msg, topic);
        }
    }

    public void sendMobilByCache(String msg, String topic) throws Exception {
        if (CACHED.get(msg + "_mobile") == null) {
            if (topic == null) {
                topic = TOPIC;
            }
            NetUtil.sendMobileBycurlD(CALLNUMBER, msg, topic);
            CACHED.put(msg + "_mobile", "TRUE");
        }
    }

    public void sendMessageByCache(String msg, String topic) throws Exception {
        sendEmailByCache(msg, topic);
        sendMobilByCache(msg, topic);
    }

    public String putCache(String key, String value) {
        return CACHED.put(getMonitorName() + key, value);
    }

    public String getCache(String key) {
        return CACHED.get(getMonitorName() + key);
    }
}
