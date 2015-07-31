package com.yihaodian.bi.common.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CmdUtil {

    private static Log LOG = LogFactory.getLog(CmdUtil.class);

    public static class Message {

        public static int   CODE_SUCCESS = 0;
        public int          code;
        public StringBuffer info;
        public String       err;

        public boolean isSuccess() {
            if (code == CODE_SUCCESS) {
                return true;
            }
            return false;
        }
    }

    public static Message run(String[] cmds, boolean info, boolean error) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Command Line : " + Arrays.toString(cmds));
        }
        Process p = Runtime.getRuntime().exec(cmds);
        int rs = p.waitFor();
        Message m = new Message();
        m.code = rs;
        if (rs != 0) {
            // p.getErrorStream();
        }
        BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = null;
        StringBuffer sb = new StringBuffer();
        while ((line = r.readLine()) != null) {
            sb.append(line).append("\n");
        }
        m.info = sb;
        return m;
    }

    public static Message run(String[] cmds) throws Exception {
        return run(cmds, true, true);
    }
}
