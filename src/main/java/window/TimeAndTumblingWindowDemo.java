package window;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import streaming.model.StaEntity;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 时间和滚动窗口示例
 * target: 每3秒求和当前窗口数据以及当前窗口有多少个元素
 * input  List(String code, long createTime, long data)
 * output: (code,sum,count)
 */
public class TimeAndTumblingWindowDemo {
    static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.fromCollection(generatorSource())//
                .assignTimestampsAndWatermarks(new MyWatermark())//
                .keyBy("code")//
                .timeWindow(Time.seconds(3))//
                .apply(new MyWindow())
                .print();
        env.execute("TimeAndTumblingWindowDemo....");
    }

    // 生成数据
    public static List<StaEntity> generatorSource() {
        // 写入数据
        List<StaEntity> data = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Double d = Math.random() * 10000;
            data.add(new StaEntity("0001", d.longValue(), d.longValue()));
        }
        return data;
    }

    // 自定义窗口方法
    public static class MyWindow implements WindowFunction<StaEntity, Tuple3<String, Long, Long>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow window, Iterable<StaEntity> input, Collector<Tuple3<String, Long, Long>> out) throws Exception {
            long sum = 0;
            long count = 0;
            System.out.println("---------------------");
            StaEntity m = new StaEntity();
            for (Iterator<StaEntity> iterator = input.iterator(); iterator.hasNext(); ) {
                count++;
                StaEntity StaEntity = iterator.next();
                // System.out.println(StaEntity);
                sum += StaEntity.getData();
                m = StaEntity;
            }
            System.out.println("-------------------");
            System.out.println("window start:" + format.format(window.getStart()) + ";window end:" + format.format(window.getEnd()));
            out.collect(new Tuple3<>(m.getCode(), sum, count));
        }
    }

    // 提取水印
    public static class MyWatermark implements AssignerWithPeriodicWatermarks<StaEntity> {
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis());
        }

        public long extractTimestamp(StaEntity element, long previousElementTimestamp) {
            return element.getCreateTime();
        }
    }
}


