package training;


import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * 基于上次的练习
 * {@link LongRides}
 */
public class LongRidesTolerance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 配置Flink重启60次在10秒的延迟下，如果job尝试60次都失败，这个失败。
        env.enableCheckpointing(100).setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10, TimeUnit.SECONDS)));
        // 设置状态存储的位置... 这个需要设置...
        env.setStateBackend(new FsStateBackend(""));

        DataStream<TaxiRide> resources = env.addSource(new CheckpointedTaxiRideSource("src/main/resources/nycTaxiRides.gz", 1800));
        DataStream<TaxiRide> result = resources.keyBy("rideId").process(new MyProcessFunction());

        result.print();
        env.execute();
    }
}
