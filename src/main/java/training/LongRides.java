package training;


import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 *   包含的结果是触发的前两个小时没有匹配END事件的出租车的start事件
 *   使用PorcessFunction
 * @author 丹丘生
 */
public class LongRides {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        int maxEventDelaySecs = 60;
        int servingSpeedFactor = 1800;

        DataStream<TaxiRide> resources = env.addSource(new TaxiRideSource("src/main/resources/nycTaxiRides.gz", maxEventDelaySecs, servingSpeedFactor));


        //过滤数据并打印
       DataStream<TaxiRide> result =  resources.filter((a) -> GeoUtils.isInNYC(a.startLon, a.startLat) && GeoUtils.isInNYC(a.endLon, a.endLat)).keyBy("rideId").process(new MyProcessFunction());

       result.print();
       env.execute("execute");
    }
}

class MyProcessFunction extends ProcessFunction<TaxiRide, TaxiRide> {

    private ValueState<TaxiRide> state;

    @Override
    public void open(Configuration configuration){
        state = getRuntimeContext().getState(new ValueStateDescriptor<TaxiRide>("rideState",TaxiRide.class));
    }

    // 处理每一个元素
    @Override
    public void processElement(TaxiRide value, Context ctx, Collector<TaxiRide> out) throws Exception {
        TimerService timerService = ctx.timerService();
        if(value.isStart){
            if(state.value()==null){
                state.update(value);
            }
        }else
            state.update(value);

        timerService.registerEventTimeTimer(value.getEventTime()+2*60*60*1000);
        //
    }

    // 触发
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TaxiRide> out) throws Exception {
            //
            TaxiRide value = state.value();
            if(value!=null&& value.isStart){
                out.collect(value);
            }
            state.clear();
    }
}