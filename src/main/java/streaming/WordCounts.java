package streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 单词统计，采用DataSet
 * target:输出该单词统计了多少次
 */
public class WordCounts {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> list = env.fromElements("To be, or not to be,--that is the question:--",
                "Whether 'tis nobler in the mind to suffer",
                "The slings and arrows of outrageous fortune",
                "Or to take arms against a sea of troubles,");
        DataSet<Tuple2<String, Integer>> data = list
                .flatMap(new LineSplit())
                .groupBy(0)// 以单词分组
                .sum(1);// 总和次数
        data.print();
    }
   private static class LineSplit implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] arr = value.split("\\W+");
            for (String str : arr) {
                if (str.length() > 0)
                    out.collect(new Tuple2<>(str, 1));
            }
        }
    }

}
