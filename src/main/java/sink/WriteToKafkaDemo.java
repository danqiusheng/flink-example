package sink;

import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import streaming.model.StaEntity;

import java.util.Random;


/**
 * 将staEntity{@link state.CheckPointDemo} 一起使用
 * target:用于容错信息
 */
public class WriteToKafkaDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StaEntity> source = env.addSource(new MySourceFunction());
        FlinkKafkaProducer010<StaEntity> sink = new FlinkKafkaProducer010("192.168.6.28:9092", "myState",
                new TypeInformationSerializationSchema<>(TypeInformation.of(StaEntity.class), env.getConfig()));
        source.addSink(sink);
        env.execute();
    }


    public static class MySourceFunction implements SourceFunction<StaEntity> {
        private Random random = new Random();
        private boolean isRunning = true;
        private static int BOUND = 100;
        private int counter = 0;

        @Override
        public void run(SourceContext<StaEntity> ctx) throws Exception {
            while (isRunning && counter < BOUND) {
                //int first = random.nextInt(BOUND / 2 - 1) + 1;
                ctx.collect(new StaEntity("001", System.currentTimeMillis() + counter * 1000, new Integer(counter).longValue()));
                counter++;
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }


}
