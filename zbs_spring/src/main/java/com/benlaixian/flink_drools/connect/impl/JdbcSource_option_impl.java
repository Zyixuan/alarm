package com.benlaixian.flink_drools.connect.impl;

import com.benlaixian.flink_drools.connect.interfaces.JDBCSource_option;
import com.benlaixian.flink_drools.utils.JDBCSource_connect_function;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JdbcSource_option_impl implements JDBCSource_option {
    public static DataStream<String> getSource(StreamExecutionEnvironment env ) {
        return env.addSource(new JDBCSource_connect_function("select * from ceshi where id>1"));
    }
}

