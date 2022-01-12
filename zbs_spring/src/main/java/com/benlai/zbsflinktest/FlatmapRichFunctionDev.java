package com.benlai.zbsflinktest;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


public class FlatmapRichFunctionDev extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>> {
    private Integer num_flag;
    //定义状态，保存上一次的值
    private ValueState<Integer> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("last_temp", Integer.class));
    }

    public FlatmapRichFunctionDev(int num_flag) {
        this.num_flag = num_flag;
    }

    @Override
    public void flatMap(Tuple2<String, Integer> stringIntegerTuple2, Collector<Tuple3<String, Integer, Integer>> collector) throws Exception {
        Integer value = state.value();
        if (value != null) {
            Integer i = Math.abs(stringIntegerTuple2.f1 - value);
            if(i > num_flag) {
                collector.collect(new Tuple3<>(stringIntegerTuple2.f0,value,stringIntegerTuple2.f1)) ;
            }

        }
        state.update(stringIntegerTuple2.f1);
    }
}
