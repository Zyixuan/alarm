package com.benlai.zbsflinktest;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class sink_Test_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> stringDataStream = env.readTextFile("E:\\java_home_work\\IdeaProjects\\zbs_learn\\zbs_spring\\src\\main\\resources\\Tess_data");
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = stringDataStream.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] split = s.split(" ");
                        for (String s1 : split) {
                            out.collect(new Tuple2<String, Integer>(s1, 1));
                        }
                    }
                }
        ).filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return !stringIntegerTuple2.f0.isEmpty() && stringIntegerTuple2.f0 != " ";
            }
        }).keyBy(0).sum(1).setParallelism(4);
        tuple2SingleOutputStreamOperator.print().setParallelism(4);

//        tuple2SingleOutputStreamOperator.addSink(
//                new FlinkKafkaProducer011<Tuple2<String, Integer>>("192.168.116.110:9092", "", new mySerializationSchema())
//        );
//        tuple2SingleOutputStreamOperator.addSink(
//                new RedisSink<Tuple2<String, Integer>>
//                (new FlinkJedisPoolConfig.Builder().setHost("werewr").setPort(1112).build(),
//                        new RedisMapper<Tuple2<String, Integer>>() {
//                            //定义保存数据到redis的命令
//                            @Override
//                            public RedisCommandDescription getCommandDescription() {
//                                return new RedisCommandDescription(RedisCommand.HSET, "sadasdas");
//                            }
//
//                            @Override
//                            public String getKeyFromData(Tuple2<String, Integer> data) {
//                                return data.f0;
//                            }
//
//                            @Override
//                            public String getValueFromData(Tuple2<String, Integer> data) {
//                                return data.f1.toString();
//                            }
//                        }
//                ));
//        //定影
//        ArrayList<HttpHost> httpHosts = new ArrayList<HttpHost>();
//        tuple2SingleOutputStreamOperator.addSink(new ElasticsearchSink.Builder<Tuple2<String,Integer>>(httpHosts, new ElasticsearchSinkFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void process(Tuple2<String, Integer> stringIntegerTuple2, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
//                //定义写入的数据 写成 kv形式 或者
//                HashMap<String, String> stringStringHashMap = new HashMap<String, String>();
//                stringStringHashMap.put(stringIntegerTuple2.f0.toString(),stringIntegerTuple2.f1.toString());
//                //创建请求,作为向es发起的写入请求
//                IndexRequest type = Requests.indexRequest().index("test").type("type").source(stringStringHashMap);
//                requestIndexer.add(type);
//
//            }
//        }).build());
        tuple2SingleOutputStreamOperator.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
            //声明连接和预编译语句
            Connection connection = null;
            PreparedStatement insertstmt = null;
            PreparedStatement updateStmnt = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
                System.out.println(connection);
                insertstmt = connection.prepareStatement("insert into ceshi(name,id) values (?,?)");
                updateStmnt = connection.prepareStatement("update  ceshi set id=? where name=?");
            }

            //每来一条数据，就调用连接执行sql


            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                //直接执行更新语句，如果没有更新就拆入
                updateStmnt.setInt(1, value.f1);
                updateStmnt.setString(2, value.f0);
                updateStmnt.execute();
                if (updateStmnt.getUpdateCount() == 0) {
                    insertstmt.setString(1,value.f0);
                    insertstmt.setInt(2,value.f1);
                    insertstmt.execute();
                }
            }

            @Override
            public void close() throws Exception {
                insertstmt.close();
                updateStmnt.close();
                connection.close();
            }
        });

        env.execute();


    }
}

class mySerializationSchema implements SerializationSchema<Tuple2<String, Integer>> {
    @Override
    public byte[] serialize(Tuple2<String, Integer> stringIntegerTuple2) {
        return stringIntegerTuple2.toString().getBytes();
    }
}


