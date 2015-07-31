package test.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import com.yihaodian.bi.storm.common.model.TrackerVo;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TrackSpout implements IRichSpout{
	
	private static final long serialVersionUID = 1083187711319639257L;

	SpoutOutputCollector _collector;
	FileReader _fileReader;
	boolean _isComplete;
	
	@Override
	public void nextTuple() {
	    if (_isComplete) {
	        try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
	    }
		BufferedReader reader = new BufferedReader(_fileReader);
		String str;
		try{
    		while((str = reader.readLine()) != null){
    			_collector.emit(new Values(new TrackerVo(str)), str);
    		}
		}catch(Exception e){
			throw new RuntimeException("Error reading tuple",e);
		}finally{
		    _isComplete = true;
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			_fileReader = new FileReader(conf.get("file").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file["+conf.get("wordFile")+"]");
		}
		_collector = collector;
		_isComplete = false;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("track"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

}
