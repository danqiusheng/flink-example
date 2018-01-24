package training;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 每五分钟计算过去十五分钟在同一地区开始并且结束的汽车数量
 */
public class PopularPlace {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        int maxEventDelaySecs = 60;
        int servingSpeedFactor = 1800;
        DataStream<TaxiRide> resources = env.addSource(new TaxiRideSource("src/main/resources/nycTaxiRides.gz", maxEventDelaySecs, servingSpeedFactor));

        DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> result = resources.filter((a) ->
                GeoUtils.isInNYC(a.startLon, a.startLat) && GeoUtils.isInNYC(a.endLon, a.endLat)
        )//
                .map(new GridCellMatch())
                .keyBy(0, 1)//
                .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5)))
                .apply(new MyWindowFunction())//
                .filter((count -> count.f3 >= 20))// 如果超过20算流行场所
                .map(new MyMapFunction());

        result.print();
        env.execute();
    }
}


class GridCellMatch implements MapFunction<TaxiRide, Tuple2<Integer, Boolean>> {

    @Override
    public Tuple2<Integer, Boolean> map(TaxiRide value) throws Exception {
        if (value.isStart) {
            return new Tuple2<>(GeoUtils.mapToGridCell(value.startLon, value.startLat), true);
        } else
            return new Tuple2<>(GeoUtils.mapToGridCell(value.endLon, value.endLat), false);
    }
}

//IN, OUT, KEY, W extends Window
class MyWindowFunction implements WindowFunction<Tuple2<Integer, Boolean>, Tuple4<Integer, Long, Boolean, Integer>, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<Integer, Boolean>> input, Collector<Tuple4<Integer, Long, Boolean, Integer>> out) throws Exception {
        int count = 0;
        for (Tuple2<Integer, Boolean> tuple2 : input) {
            count++;
        }
        int cellId = ((Tuple2<Integer, Boolean>) tuple).f0;
        long windowTime = window.getEnd();
        boolean status = ((Tuple2<Integer, Boolean>) tuple).f1;
        out.collect(new Tuple4<>(cellId, windowTime, status, count));
    }
}

class MyMapFunction implements MapFunction<Tuple4<Integer, Long, Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>> {
    @Override
    public Tuple5<Float, Float, Long, Boolean, Integer> map(Tuple4<Integer, Long, Boolean, Integer> value) throws Exception {
        return new Tuple5<>(//
                GeoUtils.getGridCellCenterLon(value.f0),//
                GeoUtils.getGridCellCenterLat(value.f0),//
                value.f1,//
                value.f2,//
                value.f3);
    }
}