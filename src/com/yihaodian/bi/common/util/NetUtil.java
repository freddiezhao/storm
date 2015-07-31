package com.yihaodian.bi.common.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yihaodian.bi.common.util.CmdUtil.Message;

public class NetUtil {
	
    private static Log LOG = LogFactory.getLog(NetUtil.class);

    public static String sendPost(String url, String param, boolean isPost) throws Exception {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            URLConnection conn = realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            out.print(param);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return result;
    }

    public static Message curlD(String url, String params) throws Exception {
        return CmdUtil.run(new String[] { "curl", url, "-d", params });
    }

    public static Message sendMobileBycurlD(String callNumbers, String content, String topic) throws Exception {
        String url = "http://oms.yihaodian.com.cn/itil/api/sendWarning";
        String params = "apiType=BIMonitor&param={" + "\"receivers\":\"" + callNumbers + "\"," + "\"sendType\":\"tel\","
                        + "\"warningMessage\":\" " + content + "\"," + "\"warningTopic\":\"" + topic + "\"}";

        Message m = NetUtil.curlD(url, params);
        if (LOG.isInfoEnabled()) {
            LOG.info("Send Email URL[ " + url + params + " ]");
            LOG.info("Send Email Return Message [ " + m.info.toString() + " ]");
        }
        if (m.info != null) {
            if (m.info.indexOf("\"succ\"") < 0) {
                m.code = 1;
            }
        }
        return m;
    }
    public static Message sendEmailBycurlD(String mailList, String content, String topic) throws Exception {
        String url = "http://oms.yihaodian.com.cn/itil/api/sendWarning";
        String params = "apiType=BIMonitor&param={" + "\"receivers\":\"" + mailList + "\"," + "\"sendType\":\"email\","
                        + "\"warningMessage\":\" " + content + "\"," + "\"warningTopic\":\"" + topic + "\"}";

        Message m = NetUtil.curlD(url, params);
        if (LOG.isInfoEnabled()) {
            LOG.info("Send Email URL[ " + url + params + " ]");
            LOG.info("Send Email Return Message [ " + m.info.toString() + " ]");
        }
        if (m.info != null) {
            if (m.info.indexOf("\"succ\"") < 0) {
                m.code = 1;
            }
        }
        return m;
    }
}

