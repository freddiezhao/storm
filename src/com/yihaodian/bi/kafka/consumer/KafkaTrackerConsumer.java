package com.yihaodian.bi.kafka.consumer;

public interface KafkaTrackerConsumer {

    public boolean hasNext();

    public String next();
}
