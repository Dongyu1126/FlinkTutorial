package com.atguigu.chapter05;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransMapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        //方法2：传入匿名类，实现MapFunction
        SingleOutputStreamOperator<String> map = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event e) throws Exception {
                return e.user;
            }
        });
        map.print();

        //方法3：传入lambda表达式
        SingleOutputStreamOperator<String> map1 = stream.map(data -> data.user);

        // 传入MapFunction的实现类
       // stream.map(new UserExtractor()).print();//map的转换算子对数据类型进行转换

        env.execute();
    }
//    public static class UserExtractor implements MapFunction<Event, String> {//方法1：自定义MapFunction,进行类型转换
//        @Override
//        public String map(Event e) throws Exception {
//            return e.user;
//        }
//    }
}

