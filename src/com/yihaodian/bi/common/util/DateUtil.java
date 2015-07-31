package com.yihaodian.bi.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

    /** yyyy-MM-dd HH:mm:ss **/
    public static final String           YYYY_MM_DD_HH_MM_SS_STR = "yyyy-MM-dd HH:mm:ss";
    public static final String           YYYYMMDDHHMM_STR        = "yyyyMMddHHmm";
    public static final String           YYYYMMDDHH_STR          = "yyyyMMddHH";
    public static final String           YYYYMMDD_STR            = "yyyyMMdd";
    public static final String           YYYY_MM_DD_STR          = "yyyy-MM-dd";
    public static final String           YYYY_MM_DD_HH_MM_STR    = "yyyy-MM-dd HH:mm";
    public static final String           HHMM                    = "HHmm";

    public static final SimpleDateFormat YYYY_MM_DD              = new SimpleDateFormat(YYYY_MM_DD_STR);
    public static final SimpleDateFormat YYYYMMDDHHMM            = new SimpleDateFormat(YYYYMMDDHHMM_STR);
    public static final SimpleDateFormat YYYYMMDDHH              = new SimpleDateFormat(YYYYMMDDHH_STR);
    public static final SimpleDateFormat YYYYMMDD                = new SimpleDateFormat(YYYYMMDDHH_STR);

    public static final long             MINUTE_FIVE             = 5 * 60 * 1000;
    public static final long             MINUTE_TEN              = 10 * 60 * 1000;

    public static String getCurDate() {
        return YYYYMMDD.format(new Date());
    }

    public static long getLastCheckPointWholeFiveMinute() {
        return getRecentMinutePoint(-5);
    }

    public static long getRecentMinutePoint(int wholeMinute) {
        Calendar c = Calendar.getInstance();
        int s = 0;
        if (wholeMinute >= 0) {
            s = ((wholeMinute - (c.get(Calendar.MINUTE) % wholeMinute)) * 60 - c.get(Calendar.SECOND));
        } else {
            s = -1 * (c.get(Calendar.MINUTE) % (wholeMinute * -1)) * 60;
        }
        c.add(Calendar.SECOND, s);

        return c.getTimeInMillis() / 1000 * 1000;
    }

    public static String transferDateToString(Date d, String format) {
        if (d == null) {
            d = new Date();
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(d);
    }

    public static Date getDate(String str, String patton) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(patton);
        if (str != null) {
            try {
                cal.setTime(sdf.parse(str));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return cal.getTime();
    }

    /**
     * 获取当前的日期
     * 
     * @return
     */
    public static Date getDate() {
        return Calendar.getInstance().getTime();
    }

    /**
     * @param format
     * @param dayNum 天数 -1表示昨天 0表示今天
     * @return
     */
    public static String transferDateToStringCal(String format, int dayNum) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, dayNum);
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(cal.getTime());
    }

    public static String getCurTime() {
        return YYYY_MM_DD.format(new Date());
    }

    // 获得前2分钟，如201405051651
    public static String getLastMinute() {
        SimpleDateFormat sdfDateFormat = new SimpleDateFormat(YYYYMMDDHHMM_STR);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, -2);
        return sdfDateFormat.format(cal.getTime());
    }

    // 获得step分钟后的时间，如201405051651，如传入负数，则step分钟前
    public static String getLastStepMinute(int step) {
        SimpleDateFormat sdfDateFormat = new SimpleDateFormat(YYYYMMDDHHMM_STR);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MINUTE, step);
        return sdfDateFormat.format(cal.getTime());
    }

    public static String getMinute(String dateString, int step) {
        SimpleDateFormat sdfDateFormat = new SimpleDateFormat(YYYYMMDDHHMM_STR);
        Calendar cal = Calendar.getInstance();
        try {
            if (dateString != null) {
                cal.setTime(sdfDateFormat.parse(dateString));
            }
            cal.add(Calendar.MINUTE, step);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return sdfDateFormat.format(cal.getTime());
    }

    // 获得当前分钟，如201405051649
    public static String getCurMinute() {
        SimpleDateFormat sdfDateFormat = new SimpleDateFormat(YYYYMMDDHHMM_STR);
        Calendar cal = Calendar.getInstance();
        return sdfDateFormat.format(cal.getTime());
    }

    // 获得分钟格式，如201405051649
    public static String getCurMinute(Date date) {
        SimpleDateFormat sdfDateFormat = new SimpleDateFormat(YYYYMMDDHHMM_STR);
        Calendar cal = Calendar.getInstance();
        if (date != null) {
            cal.setTime(date);
        }
        return sdfDateFormat.format(cal.getTime());
    }

    public static String getMinute(Date d) {
        SimpleDateFormat sdfDateFormat = new SimpleDateFormat(YYYYMMDDHHMM_STR);
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        return sdfDateFormat.format(cal.getTime());
    }

    // 获得日期天格式，格式自定义
    public static String getFmtDate(Date date, String format) {
        SimpleDateFormat sdfDateFormat = new SimpleDateFormat(format);
        Calendar cal = Calendar.getInstance();
        if (date != null) {
            cal.setTime(date);
        }
        return sdfDateFormat.format(cal.getTime());
    }

    // 获得日期格式，如2014-05-05
    public static String getCountDate(String date, String patton) {
        SimpleDateFormat sdfDateFormat = new SimpleDateFormat(patton);
        SimpleDateFormat sdfDateFormat2 = new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS_STR);
        Calendar cal = Calendar.getInstance();
        if (date != null) {
            try {
                cal.setTime(sdfDateFormat2.parse(date));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return sdfDateFormat.format(cal.getTime());
    }

    /**
     * 获取当前小时数
     * 
     * @return
     */
    public static int getCurHour() {
        Calendar cal = Calendar.getInstance();
        return cal.get(Calendar.HOUR_OF_DAY);
    }

    public static void main(String[] args) {
    }
}
