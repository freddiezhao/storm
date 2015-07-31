package com.yihaodian.bi.cached;

import java.util.HashMap;

import org.apache.commons.collections.map.LRUMap;

import com.yihaodian.bi.cached.factory.CachedFactory;
import com.yihaodian.bi.cached.factory.HBaseFactory;
import com.yihaodian.bi.common.Debug;
import com.yihaodian.bi.hbase.HBaseConstant;

public class Example {

    public static void demo4() throws Exception {
        // 异步提交 HBase 中
        // 在本地内存中缓存，优先使用本地内存查找，但不一定找的到，缓存采用 LRU ，上限为 10000
        YCacheCMap hbase = new YCacheCMap();
        AsyncCommitCMap cm = new AsyncCommitCMap(new BufferedReadCMap(new MemoryCMap(new LRUMap(10000)), hbase), true,
                                                 50);
        for (int x = 0; x < 120000; x++) {
            cm.put("key" + (x % 500), "TEST");
            cm.incr("keyinc" + (x % 50), 1);
        }
        Thread.sleep(5000);
        System.out.println(hbase.debugContent.size());
        for (int x = 0; x < 10; x++) {
            System.out.println("keyinc" + x + " = " + hbase.debugContent.get("keyinc" + x));
        }
        cm.stop();
    }

    public static void demo1() throws Exception {
        // 异步提交 HBase 中
        // 在本地内存中缓存，优先使用本地内存查找，但不一定找的到，缓存采用 LRU ，上限为 10000
        CMap cm = CachedFactory.getDefaultHBaseAsyncLRU();
        for (int x = 0; x < 12000000; x++) {
            cm.put("key" + (x % 1000000), "TEST");
            cm.incr("keyinc" + (x % 50), 1);
        }
        Thread.sleep(5000);
        for (int x = 0; x < 10; x++) {
            System.out.println("keyinc" + x + " = " + cm.get("keyinc" + x));
        }
    }

    public static void demo3() throws Exception {
        // 异步提交 HBase 中
        // 在本地内存中缓存，优先使用本地内存查找，但不一定找的到，缓存采用 LRU ，上限为 10000
        CMap hbase = HBaseFactory.getHBaseCMap(HBaseConstant.Table.DEFAULT_KV_TABLE);
        final AsyncCommitCMap cm = new AsyncCommitCMap(new BufferedReadCMap(new MemoryCMap(new LRUMap(10000)), hbase),
                                                       true, 500);
        Thread d = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    System.out.println("Thread 1 start");
                    for (int x = 0; x < 400000; x++) {
                        cm.put("key" + (x % 100000), "TEST");
                        cm.incr("keyinc" + (x % 50), 1);
                    }
                    System.out.println("Thread 1 end");
                } catch (Exception e) {
                }
            }
        });
        Thread d2 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    System.out.println("Thread 2 start");
                    for (int x = 400000; x < 800000; x++) {
                        cm.put("key" + (x % 100000), "TEST");
                        cm.incr("keyinc" + (x % 50), 1);
                    }
                    System.out.println("Thread 2 end");
                } catch (Exception e) {
                }
            }
        });
        Thread d3 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    System.out.println("Thread 3 start");
                    for (int x = 800000; x < 1200000; x++) {
                        cm.put("key" + (x % 100000), "TEST");
                        cm.incr("keyinc" + (x % 50), 1);
                    }
                    System.out.println("Thread 3 end");
                } catch (Exception e) {
                }
            }
        });
        d.start();
        d2.start();
        d3.start();
        while (true) {
            if (!d.isAlive() && !d2.isAlive()) {
                break;
            }
        }
        Thread.sleep(10000);
        for (int x = 0; x < 10; x++) {
            System.out.println("keyinc" + x + " = " + hbase.get("keyinc" + x));
        }
        cm.stop();
    }

    public static void demo2() throws Exception {
        // 同步提交到 HBase 中
        // 在本地内存中缓存，优先使用本地内存查找，一定找的到， 缓存采用 HashMap ，无上限
        YCacheCMap y = new YCacheCMap();
        CMap cm = new BufferedReadCMap(new MemoryCMap(new HashMap()), y);
        long s = System.currentTimeMillis();
        for (int x = 0; x < 500000000; x++) {
            cm.put("key" + x, "TEST");
            if (x % 1000000 == 0) {
                System.out.println("1000000 用时 = " + (System.currentTimeMillis() - s));
                s = System.currentTimeMillis();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Debug.DEBUG = true;
        // demo1();
        // demo2();
        // demo3();
        demo1();
    }
}
