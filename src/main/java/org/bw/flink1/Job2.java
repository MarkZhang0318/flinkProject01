package org.bw.flink1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
* flink入门程序
*
* 实现实时的词频统计
* 1.单词是源源不断的输入的，可以基于nc指令来模拟这一场景
*   nc -lk 9999 绑定9999端口持续输出
* 2.基于对象来封装数据
*
* */
public class Job2 {
    public static void main(String[] args) throws Exception {
        //1.创建程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.添加要处理的数据源
        DataStreamSource<String> source = env.socketTextStream("192.168.116.130", 9999);
        //3.处理数据
        SingleOutputStreamOperator<WordCount> result = source.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String s, Collector<WordCount> collector) throws Exception {
                String[] words = s.split(",");
                //Write
                for (String word : words) {
                    collector.collect(new WordCount(word, 1));
                }
            }
        }).keyBy(t -> t.getWord()).sum("count");

        //4.输出结果
        result.print();

        //5.启动程序
        env.execute("job2");


    }

    public static class WordCount {
        private String word;
        private int count;

        public WordCount() {

        }

        public WordCount(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
