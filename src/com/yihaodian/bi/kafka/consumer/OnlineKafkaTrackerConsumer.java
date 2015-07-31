package com.yihaodian.bi.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import com.yihaodian.bi.common.Debug;
import com.yihaodian.bi.storm.common.util.Constant;

public class OnlineKafkaTrackerConsumer implements KafkaTrackerConsumer {

    public static String[]           DEBUGSTR   = new String[] { "a\nb\nc\n", "d", "e\nf\n" };
    public static int                DEBUGINDEX = -1;

    KafkaStream<byte[], byte[]>      kafkaStream;
    ConsumerIterator<byte[], byte[]> kafkaStreamIt;
    String[]                         tracks     = null;
    int                              trackIndex = 0;

    public OnlineKafkaTrackerConsumer(String groupId, String topic){
        ConsumerConnector kafkaConsumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(groupId));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = kafkaConsumer.createMessageStreams(topicCountMap);
        kafkaStream = consumerMap.get(topic).get(0);
        kafkaStreamIt = kafkaStream.iterator();
    }

    private ConsumerConfig createConsumerConfig(String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", Constant.KAFKA_ZK_CONNECTION);
        props.put("group.id", groupId);
        // props.put("group.id", "bi_test_comsumerName");
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "10000");
        return new ConsumerConfig(props);
    }

    @Override
    public synchronized boolean hasNext() {
        if (tracks == null || trackIndex > tracks.length - 1) {
            tracks = null;
            trackIndex = 0;
            return kafkaStreamIt.hasNext();
        } else {
            return true;
        }
    }

    @Override
    public synchronized String next() {
        if (tracks == null) {
            tracks = new String(kafkaStreamIt.next().message()).split("\\n");
        }
        return tracks[trackIndex++];
    }
}
