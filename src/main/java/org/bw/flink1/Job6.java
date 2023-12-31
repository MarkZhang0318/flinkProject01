package org.bw.flink1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
* 设置并行度，并从webUI检查程序运行情况
*
* 而后设置全局并行度为2，继续观察
*
* */
public class Job6 {
    public static void main(String[] args) throws Exception {
        //1.创建程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        //2.添加要处理的数据源

        DataStreamSource<String> source = env.socketTextStream("192.168.117.130", 9999);
        //3.处理数据
        SingleOutputStreamOperator<WordCount> result = source.flatMap(new Job3WordCount()).keyBy(t -> t.getWord()).sum("count");

        //4.输出结果
        result.print();

        //5.启动程序
        env.execute("job4");


    }

    public static class Job3WordCount implements FlatMapFunction<String, WordCount> {
        @Override
        public void flatMap(String value, Collector<WordCount> out) throws Exception {
            String[] words = value.split(",");
            //Write
            for (String word : words) {
                out.collect(new WordCount(word, 1));
            }
        }
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
