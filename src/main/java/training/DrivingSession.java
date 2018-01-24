package training;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.GapSegment;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ConnectedCarAssigner;
import com.dataartisans.flinktraining.exercises.datastream_java.windows.DrivingSessions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *
 */
public class DrivingSession {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取每一行的记录
        DataStream<String> carData = env.readTextFile("src/main/resources/carOutOfOrder.csv");

        // create event stream
        DataStream<ConnectedCarEvent> events = carData
                .map(new MapFunction<String, ConnectedCarEvent>() { // 将每一行的记录转成ConnectedCarEvent
                    @Override
                    public ConnectedCarEvent map(String line) throws Exception {
                        return ConnectedCarEvent.fromString(line);
                    }
                })
                .assignTimestampsAndWatermarks(new ConnectedCarAssigner());
        events.keyBy("carId").window(EventTimeSessionWindows.withGap(Time.seconds(15)))
                .apply(new DrivingSessions.CreateGapSegment())//
                .print();

        env.execute("driving session....");
    }
}


class CreateGapSegment implements WindowFunction<ConnectedCarEvent, GapSegment, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<ConnectedCarEvent> events, Collector<GapSegment> out) {
        out.collect(new GapSegment(events));
    }

}
