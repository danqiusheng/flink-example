package training.eventjoin;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Customer;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.EnrichedTrade;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Trade;
import com.dataartisans.flinktraining.exercises.datastream_java.process.EventTimeJoinFunction;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.FinSources;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LowLatencyEventTimeJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Trade> tradeStream = FinSources.tradeSource(env);

        DataStream<Customer> customerStream = FinSources.customerSource(env);

        DataStream<EnrichedTrade> joinedStream = tradeStream
                .keyBy("customerId")
                .connect(customerStream.keyBy("customerId"))
                .process(new EventTimeJoinFunction());

        joinedStream.print();

        env.execute("Low-latency event-time join");
    }
}
