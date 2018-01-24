package multi;


import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import streaming.model.StaEntity;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;


/**
 * select t.*, p.* from t join p on t.id = p.id
 * target:join 两个数据源
 * in:(code,data) (code,data)
 * output:(code1,data1,code2,data2)
 */
public class JoinDataSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        List<StaEntity> data = generatorSource();
        DataStream<StaEntity> source1_1 = env.fromCollection(data).assignTimestampsAndWatermarks(new MyWatermark());
        // 生成源2
        List<StaEntity> dataList = new ArrayList<StaEntity>();
        for (StaEntity staEntity : data) {
            Double d = Math.random() * 1000;
            StaEntity two = null;
            if (staEntity.getData() % 2 == 0) {
               two = new StaEntity("0001", staEntity.getCreateTime(), d.longValue());
            } else {
               two = new StaEntity("0002", staEntity.getCreateTime(),d.longValue());
            }
            dataList.add(two);
        }


        DataStream<StaEntity> source2_1 = env.fromCollection(dataList).assignTimestampsAndWatermarks(new MyWatermark());

        // 不同类型可以进行join 操作,示例为同一类型
        source1_1.join(source2_1)
                .where(new MyKeySelector())
                .equalTo(new MyKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new MyWindowFunction())
                .print();
        env.execute("JoinDataSourceDemo");
    }

    private static class MyWindowFunction implements FlatJoinFunction<StaEntity, StaEntity, Tuple4<String, Long,String,Long>> {
        public void join(StaEntity first, StaEntity second, Collector<Tuple4<String, Long,String,Long>> out) throws Exception {
            if (first.getCode().equals(second.getCode())) {
                out.collect(new Tuple4<>(first.getCode(), first.getData() ,second.getCode(), second.getData()));
            }
        }
    }

    private static class MyKeySelector implements KeySelector<StaEntity, String> {
        @Override
        public String getKey(StaEntity value) throws Exception {
            // System.out.println(value);
            return value.getCode();
        }
    }

    private static class MyWatermark implements AssignerWithPeriodicWatermarks<StaEntity> {
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis() - 2000);
        }

        @Override
        public long extractTimestamp(StaEntity element, long previousElementTimestamp) {
            return element.getCreateTime();
        }
    }


    private static List<StaEntity> generatorSource() {
        List<StaEntity> data = new ArrayList<StaEntity>();
        for (int i = 0; i < 15; i++) {
            Double d = Math.random() * 1000;
            StaEntity staEntity = null;
            if (i % 2 == 0) {
                staEntity = new StaEntity("0001", (System.currentTimeMillis() + 1000 * i), d.longValue());
            } else
                staEntity = new StaEntity("0002", (System.currentTimeMillis() + 1000 * i), d.longValue());
            data.add(staEntity);
        }
        return data;
    }


}
