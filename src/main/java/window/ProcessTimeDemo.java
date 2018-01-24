package window;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import streaming.model.StaEntity;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 处理时间示例
 * target: 处理时间
 */
public class ProcessTimeDemo {
    static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StaEntity> sources = env.fromCollection(generatorSource());
        sources.keyBy("code")
                .timeWindow(Time.milliseconds(1))
                .apply(new MyWindow())
                .print();
        env.execute("ProcessTimeDemo");
    }


    private static List<StaEntity> generatorSource() {
        List<StaEntity> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Double d = Math.random() * 100;
            data.add(new StaEntity("0001", (System.currentTimeMillis() + 1000 * d.intValue()), d.longValue()));
        }
        return data;
    }

    public static class MyWindow implements  WindowFunction<StaEntity, Tuple2<String, String>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow window, Iterable<StaEntity> input, Collector<Tuple2<String, String>> out) throws Exception {

            int sum = 0;
            System.out.println("----------------");
            for (Iterator<StaEntity> iterator = input.iterator(); iterator.hasNext(); ) {
                StaEntity StaEntity = iterator.next();
                sum++;
             //   System.out.println(StaEntity);
            }
            System.out.println("-----------------");
            System.out.println("sum:" + sum);
            out.collect(new Tuple2<>("window start:" + format.format(window.getStart()), "window end :" + format.format(window.getEnd())));
        }
    }
}
