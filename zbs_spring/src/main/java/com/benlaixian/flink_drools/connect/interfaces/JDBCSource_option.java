package com.benlaixian.flink_drools.connect.interfaces;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface JDBCSource_option {
    public static DataStream<String> getSource(StreamExecutionEnvironment env) {
        return null;
    }


}
