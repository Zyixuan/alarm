package com.benlai.zbsflinktest;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTest1_KeyedStateApplicationcase {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stringDataStreamSource = env.readTextFile("E:\\java_home_work\\IdeaProjects\\zbs_learn\\zbs_spring\\src\\main\\resources\\test_date");
        DataStream<Tuple2<String, Integer>> splitDataStream = stringDataStreamSource.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String o : s1) {
                    collector.collect(new Tuple2<String, Integer>(o, 1));
                }
            }
        });
        splitDataStream.keyBy(0).flatMap(new FlatmapRichFunctionDev(10));
    }
}

