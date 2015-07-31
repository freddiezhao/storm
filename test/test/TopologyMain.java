package test;

import test.spout.TrackSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.yihaodian.bi.common.Debug;
import com.yihaodian.bi.storm.logic.tracker.bolt.UVShuffleBolt;
import com.yihaodian.bi.storm.logic.tracker.bolt.UvCacheBolt;
import com.yihaodian.bi.storm.logic.tracker.spout.KafkaTrackerSpout;


public class TopologyMain {

	public static void main(String[] args) {
		Debug.DEBUG = true;
		
		LocalCluster cluster = new LocalCluster();
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TrackSpout(), 1);
    	builder.setBolt("filter", new UVShuffleBolt(), 2).shuffleGrouping("spout");
		builder.setBolt("cache", new UvCacheBolt(), 5).fieldsGrouping("filter", new Fields("guid"));
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.put("file", "sample_1k.dat");
        cluster.submitTopology("uv", conf, builder.createTopology());
	}
}
