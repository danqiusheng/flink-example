package multi;


import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import streaming.model.StaEntity;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 采用CoGroup 模拟左右外连接
 */
public class JoinDataSourceTwoDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        List<StaEntity> data = new ArrayList<StaEntity>();
        DataStream<StaEntity> source1 = env.fromCollection(generatorSource(data));
        DataStream<StaEntity> source1_1 = source1.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<StaEntity>() {
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis());
            }

            @Override
            public long extractTimestamp(StaEntity element, long previousElementTimestamp) {
                return element.getCreateTime();
            }
        });
        List<StaEntity> dataList = new ArrayList<StaEntity>();
        for (streaming.model.StaEntity StaEntity : data) {
            if (StaEntity.getData() % 2 == 0) {
                dataList.add(new StaEntity("0001", StaEntity.getCreateTime(), StaEntity.getData()));
            } else {
                dataList.add(new StaEntity("0001", StaEntity.getCreateTime(), StaEntity.getData()));
            }
        }

        DataStream<StaEntity> source2 = env.fromCollection(dataList);
        DataStream<StaEntity> source2_1 = source2.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<StaEntity>() {
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis());
            }

            @Override
            public long extractTimestamp(StaEntity element, long previousElementTimestamp) {
                return element.getCreateTime();
            }
        });
        // 不同类型可以进行join 操作
        CoGroupedStreams.WithWindow<StaEntity, StaEntity, String, TimeWindow> joinedStreams = source1_1.
                coGroup(source2_1).
                where(new StaEntityKey()).equalTo(new StaEntityKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(3)));

        DataStream<Tuple3<String, Long, String>> leftResult = joinedStreams.apply(new LeftJoinFunction());
        DataStream<Tuple5<String, String, Long, Long, String>> innerResult = joinedStreams.apply(new InnerJoinFunction());
        DataStream<Tuple3<String, Long, String>> rightResult = joinedStreams.apply(new RightJoinFunction());

        leftResult.print();
        innerResult.print();
        rightResult.print();

        env.execute("3 type data stream join ");
    }

    private static List<StaEntity> generatorSource(List<StaEntity> data) {
        for (int i = 0; i < 15; i++) {
            Double d = Math.random() * 100;
            if (i % 2 == 0) {
                data.add(new StaEntity("0001", (System.currentTimeMillis() + 1000 * i), d.longValue()));
            } else
                data.add(new StaEntity("0001", (System.currentTimeMillis() + 1000 * i), d.longValue()));

        }
        return data;
    }
}

class StaEntityKey implements KeySelector<StaEntity, String> {

    @Override
    public String getKey(StaEntity value) throws Exception {
        return value.getCode();
    }
}

class InnerJoinFunction implements CoGroupFunction<StaEntity, StaEntity, Tuple5<String, String, Long, Long, String>> {

    @Override
    public void coGroup(Iterable<StaEntity> first, Iterable<StaEntity> second, Collector<Tuple5<String, String, Long, Long, String>> out) throws Exception {
        for (Iterator<StaEntity> iterator = second.iterator(); iterator.hasNext(); ) {
            StaEntity one = iterator.next();
            for (Iterator<StaEntity> iteratorSecond = first.iterator(); iteratorSecond.hasNext(); ) {
                StaEntity staEntity = iteratorSecond.next();
                out.collect(new Tuple5<>(one.getCode(), staEntity.getCode(), one.getData(), staEntity.getData(), "inner join"));
            }
        }

    }
}


class LeftJoinFunction implements CoGroupFunction<StaEntity, StaEntity, Tuple3<String, Long, String>> {

    @Override
    public void coGroup(Iterable<StaEntity> first, Iterable<StaEntity> second, Collector<Tuple3<String, Long, String>> out) throws Exception {
        int sum = 0;
        for (streaming.model.StaEntity StaEntity : second) {
            sum++;
        }
        for (Iterator<StaEntity> iterator = first.iterator(); iterator.hasNext() && sum != 0; ) {
            StaEntity StaEntity = iterator.next();
            out.collect(new Tuple3<>(StaEntity.getCode(), StaEntity.getData(), "left join "));
        }
    }
}


class RightJoinFunction implements CoGroupFunction<StaEntity, StaEntity, Tuple3<String, Long, String>> {

    @Override
    public void coGroup(Iterable<StaEntity> first, Iterable<StaEntity> second, Collector<Tuple3<String, Long, String>> out) throws Exception {
        int sum = 0;
        for (streaming.model.StaEntity StaEntity :first) {
            sum++;
        }
        for (Iterator<StaEntity> iterator = second.iterator(); iterator.hasNext() && sum != 0; ) {
            StaEntity StaEntity = iterator.next();
            out.collect(new Tuple3<>(StaEntity.getCode(), StaEntity.getData(), "right join "));
        }
    }
}