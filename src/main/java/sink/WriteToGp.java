package sink;


import database.GPSink;
import database.GPSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streaming.model.Model;

/**
 * 自定义sink
 * target:将数据写入到GP
 */
public class WriteToGp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Model> source = env.addSource(new GPSource());
        source.addSink(new GPSink());
        env.execute();
    }


}
