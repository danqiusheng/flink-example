package training;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 将数据写入到ES
 */
public class PopularPlaceToEs {
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
                .filter((count -> count.f3 >=20))// 如果超过20算流行场所
                .map(new MyMapFunction());


        Map<String,String> config = new HashMap<>();
        config.put("bulk.flush.max.actions","1") ;// 在每个记录插入后刷新
        config.put("cluster.name","elasticsearch");

        List<InetSocketAddress> transports = new ArrayList<>();
        transports.add(new InetSocketAddress(InetAddress.getByName("192.168.6.25"),9300));
        result.addSink(new ElasticsearchSink<>(config,transports,new Inserter()));
        //result.print();
        env.execute();
    }

}

class Inserter implements ElasticsearchSinkFunction<Tuple5<Float, Float, Long, Boolean, Integer>>{

    @Override
    public void process(Tuple5<Float, Float, Long, Boolean, Integer> record, RuntimeContext runtimeContext, RequestIndexer indexer) {
        Map<String,String> json = new HashMap<String,String>();
        json.put("time",record.f2.toString());
        json.put("location",record.f1+","+record.f0);
        json.put("isState",record.f3.toString());
        json.put("cnt",record.f4.toString());

        IndexRequest rqst = Requests.indexRequest().index("nyc-places").type("popular-locations").source(json);
        indexer.add(rqst);
    }
}

