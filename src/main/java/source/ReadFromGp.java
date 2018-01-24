package source;


import database.GPSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streaming.model.Model;

/**
 * 自定义数据源
 * target: 从GP中读取数据并统计
 * 表:target_lib
 */
public class ReadFromGp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Model> source = env.addSource(new GPSource());
        source.map(new MyMapFunction())
                .keyBy(0)
                .sum(1)
                .print();
        env.execute("ReadFromGp");
    }

    private static class MyMapFunction implements MapFunction<Model, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Model value) throws Exception {
            return new Tuple2<>(value.getObjtype(), 1);
        }
    }
}
