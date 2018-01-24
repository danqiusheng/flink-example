package training;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 需求： 将数据流中不是在纽约市的数据给清洗掉，然后打印出数据
 *
 * @author 丹丘生
 */
public class RideCleansing {
    public static void main(String[] args) throws Exception {
        // 设置source
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 数据源
        /*
        For these exercises, a speed-up factor of
        600 or more (i.e., 10 minutes of event time for every second of processing),
         and a maximum delay of 60 (seconds) will work well.
         http://training.data-artisans.com/exercises/taxiData.html
         */
        int maxEventDelaySecs = 60;
        int servingSpeedFactor = 600;
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource("src/main/resources/nycTaxiRides.gz", maxEventDelaySecs, servingSpeedFactor));

        /**
         * 过滤数据
         */
       DataStream<TaxiRide> data =  rides.filter((a) ->
            GeoUtils.isInNYC(a.startLon, a.startLat) && GeoUtils.isInNYC(a.endLon, a.endLat)
        );

       data.print();

       env.execute("taxi ride cleasing...");
    }
}
