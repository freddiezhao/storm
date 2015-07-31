package com.yihaodian.bi.hbase.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class ConfigurationFactory {

	private static Configuration configuration  = null ;
	
	public static Configuration getConfiguration()
	{
		configuration = HBaseConfiguration.create() ;
		String filePath = "hbase-site.xml" ;
		Path path = new Path(filePath);
		configuration.addResource(path);
		return configuration ;
	}
}
