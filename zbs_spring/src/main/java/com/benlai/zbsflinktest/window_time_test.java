package com.benlai.zbsflinktest;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingAlignedProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class window_time_test {
    public static void main(String[] args) throws Exception {
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
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<String, Integer> element) {
                return element.f1;
            }
        });
        splitDataStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(2),Time.seconds(1)));
        splitDataStream.keyBy(0).window(SlidingEventTimeWindows.of(Time.seconds(2),Time.seconds(1),Time.seconds(1)));
        DataStream<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = splitDataStream.keyBy(0);
//                .timeWindow(Time.seconds(2)
//        ).apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
//            @Override
//            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
//                long end = window.getEnd();
//                ArrayList<Tuple2<String, Integer>> list = IteratorUtils.toList(input.iterator();
//                out.collect(list.get(0));
//            }
//        })
        tuple2SingleOutputStreamOperator.print();
        System.out.println("main");
        env.execute();

    }
}
