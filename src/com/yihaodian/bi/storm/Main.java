package com.yihaodian.bi.storm;

import org.apache.thrift7.TException;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.NotAliveException;

public class Main {
	
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException,
    											  NotAliveException, TException, InterruptedException{
        TopoSubmitter.submit(args);
    }
}


