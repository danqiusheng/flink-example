package window;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import streaming.model.StaEntity;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * countWindow示例,没有使用键控窗口
 * target:count如何使用
 */
public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromCollection(generatorSource()).countWindowAll(3, 2).apply(new MyCountWindow()).print();
        env.execute("CountWindowDemo");
    }

    private static List<StaEntity> generatorSource() {
        List<StaEntity> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Double d = Math.random() * 10000;
            data.add(new StaEntity("0001", d.longValue(), d.longValue()));
        }
        return data;
    }

    private static class MyCountWindow implements AllWindowFunction<StaEntity, Tuple3<String, Long, Integer>, GlobalWindow> {
        public void apply(GlobalWindow window, Iterable<StaEntity> values, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
            long sum = 0;
            System.out.println("----------------");
            int count = 0;
            for (Iterator<StaEntity> iterator = values.iterator(); iterator.hasNext(); ) {
                StaEntity StaEntity = iterator.next();
                sum += StaEntity.getData();
                System.out.println(StaEntity);
                count++;
            }
            System.out.println("---------------");
            out.collect(new Tuple3<>("sum", sum, count));
        }
    }
}
