package window;


import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import streaming.model.StaEntity;

/**
 * 窗口和水印示例：
 * target: 与TimeAndTumblingWindowDemo对比迟到元素处理
 * input  List(String code, long createTime, long data)
 * output: (code,sum,count)
 */
public class TimeAndWindowAndWatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.fromCollection(TimeAndTumblingWindowDemo.generatorSource())
                .assignTimestampsAndWatermarks(new MyWatermark())
                .keyBy("code")
                .timeWindow(Time.seconds(3))
                .apply(new TimeAndTumblingWindowDemo.MyWindow())
                .print();

        env.execute("TimeAndWindowAndWatermark Demo");

    }
    private static class MyWatermark implements AssignerWithPeriodicWatermarks<StaEntity> {
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis() - 2000); //延迟2秒
        }

        public long extractTimestamp(StaEntity element, long previousElementTimestamp) {
            return element.getCreateTime();
        }
    }
}
