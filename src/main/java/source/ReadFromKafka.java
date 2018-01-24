package source;


import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import streaming.model.Model;

import java.util.Properties;

/**
 * 自定义数据源
 * target:从kafka中读取数据
 */
public class ReadFromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.6.28:9092");
        properties.setProperty("group.id", "myGroup");// 消费者的分组id
        properties.setProperty("auto.offset.reset", "earliest"); // 总是从开头读取消息
        env.addSource(new FlinkKafkaConsumer010<>("target_lib",//
                      new TypeInformationSerializationSchema<>(TypeInformation.of(Model.class),env.getConfig()),
                      properties))
                .rebalance()
                .print();
        env.execute("ReadFromKafka");
    }
}
