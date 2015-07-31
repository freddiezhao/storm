package com.yihaodian.bi.kafka.consumer;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class OfflineConsumer implements KafkaTrackerConsumer {

    BufferedReader reader;
    String         str;

    public OfflineConsumer(String file){
        try {
            reader = readZipFile(file).get(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized boolean hasNext() {
        try {
            str = reader.readLine();
            if (str == null) {
                reader.close();
                return false;
            }
            return true;
        } catch (IOException e) {
        }
        return false;
    }

    @Override
    public synchronized String next() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
        }
        return str;
    }

    public static List<BufferedReader> readZipFile(String file) throws Exception {
        List<BufferedReader> brs = new ArrayList<BufferedReader>();
        ZipFile zf = new ZipFile(file);
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        ZipInputStream zin = new ZipInputStream(in);
        ZipEntry ze;
        while ((ze = zin.getNextEntry()) != null) {
            if (ze.isDirectory()) {
            } else {
                long size = ze.getSize();
                if (size > 0) {
                    brs.add(new BufferedReader(new InputStreamReader(zf.getInputStream(ze))));
                }
            }
        }
        zin.closeEntry();
        return brs;
    }
}
