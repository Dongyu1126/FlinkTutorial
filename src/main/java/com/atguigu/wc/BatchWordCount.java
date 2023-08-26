package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.从文件读取数据
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");
        //3.将每行数据进行分词，并转换成二元组类型  输入数据为String类型，Collector收集器收集成Tuple2二元组类型，并将收集器命名为out
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    //将一行文本进行分词
                    String[] words = line.split(" ");
                    //将每个单词转换成二元组输出
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));//flatMap扁平映射，没调用一个word就collect进行一次输出，使其打散输出  Tuple2.of构建二元组
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));//因为lamda表达式，会导致泛型擦除的效果，需要returns一个类型声明

        //4.按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);
        //5.分组内进行聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);
        //6.打印结果
        sum.print();

    }
}
