package window;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import streaming.model.StaEntity;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 时间和Session Windows示例
 * target:session window的时间间隔gap
 * [最前的时间,最后的时间+gap)
 */
public class TimeAndSessionWindowDemo {
    static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.fromCollection(generatorSource())
                .assignTimestampsAndWatermarks(new MyWartermark())
                .keyBy("code")
                .window(EventTimeSessionWindows.withGap(Time.seconds(8)))
                .apply(new MySessionWindow())
                .print();
        env.execute();
    }

    private static class MyWartermark implements AssignerWithPeriodicWatermarks<StaEntity> {
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis());
        }

        public long extractTimestamp(StaEntity element, long previousElementTimestamp) {
            return element.getCreateTime();
        }
    }

    private static List<StaEntity> generatorSource() {
        // 写入数据
        List<StaEntity> data = new ArrayList<StaEntity>();
        for (int i = 0; i < 15; i++) {
            Double d = Math.random() * 100;
            data.add(new StaEntity("0001", (System.currentTimeMillis() + 1000 * d.intValue()), d.longValue()));
        }
        return data;
    }


    public static class MySessionWindow implements  WindowFunction<StaEntity, Tuple2<String, String>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow window, Iterable<StaEntity> input, Collector<Tuple2<String, String>> out) throws Exception {

            int sum = 0;
            System.out.println("----------------");
            for (Iterator<StaEntity> iterator = input.iterator(); iterator.hasNext(); ) {
                StaEntity staEntity = iterator.next();
                sum++;
                System.out.println(staEntity);
            }
            System.out.println("-----------------");
            System.out.println("sum:" + sum);
            out.collect(new Tuple2<>("window start:" + format.format(window.getStart()), "window end :" + format.format(window.getEnd())));
        }
    }
}
