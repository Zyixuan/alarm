package com.benlai.zbsflinktest;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

public class flink_cep {
    public static void main(String[] args) {
        Pattern<String, String> start = Pattern.begin("start");
        start.times(3).where(new SimpleCondition<String>() {
            public boolean filter(String o ) throws Exception {
                return o.isEmpty();
            }
        }).or(
                new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.contains("ceshi");
                    }
                }
        )
        .followedBy("asdsa");

    }
}
