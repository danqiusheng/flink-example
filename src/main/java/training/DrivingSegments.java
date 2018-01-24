package training;


import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.StoppedSegment;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ConnectedCarAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 *  自定义窗口实现
 */
public class DrivingSegments {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> carData = env.readTextFile("src/main/resources/carInOrder.csv");

        DataStream<ConnectedCarEvent> events = carData
                .map(new MapFunction<String, ConnectedCarEvent>() {
                    @Override
                    public ConnectedCarEvent map(String line) throws Exception {
                        return ConnectedCarEvent.fromString(line);
                    }
                })
                .assignTimestampsAndWatermarks(new ConnectedCarAssigner());

            events.keyBy("carId").window(GlobalWindows.create())
                    .trigger(new SegmentingOutOfOrderTrigger())
                    .evictor(new SegmentingEvictor())
                    .apply(new CreateStoppedSegment())
                    .print();

            env.execute("execute ...");

    }



    // 自定义触发器
    public static class SegmentingOutOfOrderTrigger extends Trigger<ConnectedCarEvent,GlobalWindow>{
        @Override
        public TriggerResult onElement(ConnectedCarEvent element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
           if(element.speed==0.0){
               ctx.registerEventTimeTimer(element.timestamp);
           }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {

        }
    }


    // 自定义Evictor
    public static class SegmentingEvictor implements Evictor<ConnectedCarEvent,GlobalWindow>{
        @Override
        public void evictBefore(Iterable<TimestampedValue<ConnectedCarEvent>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {

        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<ConnectedCarEvent>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
            long firstStop = ConnectedCarEvent.earliestStopElement(elements);
            for (Iterator<TimestampedValue<ConnectedCarEvent>> iterator = elements.iterator(); iterator.hasNext(); ) {
                TimestampedValue<ConnectedCarEvent> element = iterator.next();
                if (element.getTimestamp() <= firstStop) {
                    iterator.remove();
                }
            }
        }
    }


    public static class CreateStoppedSegment implements WindowFunction<ConnectedCarEvent,StoppedSegment,Tuple,GlobalWindow>{

        @Override
        public void apply(Tuple tuple, GlobalWindow window, Iterable<ConnectedCarEvent> input, Collector<StoppedSegment> out) throws Exception {
                StoppedSegment seg = new StoppedSegment(input);
                if(seg.length>0){
                    out.collect(seg);
                }
        }
    }
}
