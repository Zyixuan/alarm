package com.benlai.zbsflinktest;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FlinkSteamingApi {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    StreamExecutionEnvironment.createLocalEnvironment(1)
//    StreamExecutionEnvironment.createRemoteEnvironment("172.168.72.64",4044,"")
        env.setParallelism(4);
        Properties properties = new Properties();
        properties.setProperty("", "");
        DataStreamSource<String> inputData = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), properties));
//        SingleOutputStreamOperator<Int> mapDataStream =
        SingleOutputStreamOperator<Integer> sum = inputData.filter(
                new FilterFunction<String>() {
            public boolean filter(String o) throws Exception {
                boolean b = o.isEmpty();
                return !b;
            }
        }).flatMap(new FlatMapFunction<String, Integer>() {
            public void flatMap(String o, Collector collector) throws Exception {
                String[] split = o.split("");
                for (String data : split) {
                    collector.collect(new Tuple2(data, 1));
                }
            }
        })
                .keyBy(0).sum(1);
        sum.print();
        try {
            env.execute("cveshi");
        } catch (Exception e) {
            e.getMessage();
        }

    }
}
