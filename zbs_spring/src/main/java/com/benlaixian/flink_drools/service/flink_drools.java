package com.benlaixian.flink_drools.service;

import com.benlaixian.flink_drools.connect.impl.JdbcSource_option_impl;
import com.benlaixian.flink_drools.connect.interfaces.JDBCSource_option;
import com.benlaixian.flink_drools.utils.JDBCSource_connect_function;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class flink_drools {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source = JdbcSource_option_impl.getSource(env);
        source.print();
        env.execute();
    }
}
