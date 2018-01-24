package streaming;


import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * WindowAll 转换
 */
public class WindowAllDemo {
    static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.generateSequence(10, 100000)
                .windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(100)))
                .apply(new MyAllWindowFunction())
                .print();
        env.execute();
    }

    private static class MyAllWindowFunction implements AllWindowFunction<Long, Tuple1<Long>, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<Long> values, Collector<Tuple1<Long>> out) throws Exception {
            Long sum = 0L;
            for (Long value : values) {
                sum += value;
            }
            System.out.println("start window:" +format.format(window.getStart())+";end window:"+format.format(window.getEnd()));
            out.collect(new Tuple1<>(sum));
        }
    }
}
