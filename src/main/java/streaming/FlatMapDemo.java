package streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 采用FlatMap
 * 将一个元素分割成一个或者多个元素然后统计元素
 * FlatMapFunction<T,O>
 * 参数T：输入的元素的类型
 * 参数0：输出的元素的类型
 * 要实现的方法：
 * void flatMap(T value, Collector<O> out) throws Exception;
 */
public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> data = env.fromElements("To be, or not to be,--that is the question:--",
                "Whether 'tis nobler in the mind to suffer",
                "The slings and arrows of outrageous fortune",
                "Or to take arms against a sea of troubles,");
        data.flatMap(new SplitFunction())
                .keyBy(0)
                .sum(1)
                .print();// 分组后然后对键的次数进行统计;
        // 打印结果
        env.execute("单词计数..");
    }

    private static class SplitFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] arr = value.split("\\W+");
            for (String str : arr) {
                if (str.length() > 0) {
                    out.collect(new Tuple2<>(str, 1));
                }
            }
        }
    }
}


