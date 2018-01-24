package sink;

import database.GPSource;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import streaming.model.Model;

/**
 * 自定义Sink
 * target:从gp读取数据 ，写入kafka
 */
public class WriteToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Model> source = env.addSource(new GPSource());
        FlinkKafkaProducer010<Model> sink = new FlinkKafkaProducer010("192.168.6.28:9092", "target_lib",
                  new TypeInformationSerializationSchema<>(TypeInformation.of(Model.class), env.getConfig()));
        source.addSink(sink);
        env.execute();
    }
}
