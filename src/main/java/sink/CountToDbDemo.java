package sink;


import database.MysqlSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import streaming.model.Count;
import streaming.model.Model;

import java.util.Properties;

/**
 * 统计示例，将结果写入到数据库，然后页面查询
 * 单个数据源进行统计
 * 从kafka读取数据,然后将数据统计写入到Mysql
 */
public class CountToDbDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.6.25:9092");
        properties.setProperty("group.id", "myGroup");// 消费者的分组id
        properties.setProperty("auto.offset.reset", "earliest"); // 总是从开头读取消息

        DataStream<Model> source = env.addSource(new FlinkKafkaConsumer010<>("target_lib",//
                             new TypeInformationSerializationSchema<>(TypeInformation.of(Model.class),//
                             env.getConfig()), //
                             properties));

       DataStream<Tuple4<Integer,Long,String,String>> result = source.map(new MyMap()).keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(3))).sum(1);

        //result.map(new TransformationMap()).addSink(new MysqlSink());

        result.map(new Color1Map()).keyBy(0).sum(1).map(new Color1TransMap()).addSink(new MysqlSink());

        //result.map(new Color2Map()).keyBy(0).sum(1).map(new Color2TransMap()).addSink(new MysqlSink());

        env.execute("this sink to mysql");
    }


    private static class Color1TransMap implements MapFunction<Tuple2<String,Long>,Count>{
        @Override
        public Count map(Tuple2<String, Long> value) throws Exception {
            return new Count(value.f0==null?"没颜色":value.f0,value.f1);
        }
    }
    private static class Color2TransMap implements MapFunction<Tuple2<String,Long>,Count>{
        @Override
        public Count map(Tuple2<String, Long> value) throws Exception {
            return new Count(value.f0==null?"没颜色2":value.f0,value.f1);
        }
    }

    private static class TransformationMap implements  MapFunction<Tuple4<Integer,Long,String,String>,Count>{

        @Override
        public Count map(Tuple4<Integer, Long, String, String> value) throws Exception {
            return new Count(value.f0+"",value.f1);
        }
    }

    private static class Color1Map implements  MapFunction<Tuple4<Integer,Long,String,String>,Tuple2<String,Long>>{
        @Override
        public Tuple2<String, Long> map(Tuple4<Integer, Long, String, String> value) throws Exception {
            return new Tuple2<>(value.f2,1L);
        }
    }
    private static class Color2Map implements  MapFunction<Tuple4<Integer,Long,String,String>,Tuple2<String,Long>>{
        @Override
        public Tuple2<String, Long> map(Tuple4<Integer, Long, String, String> value) throws Exception {
            return new Tuple2<>(value.f3,1L);
        }
    }


    private static class MyMap implements MapFunction<Model,Tuple4<Integer,Long,String,String>>{

        @Override
        public Tuple4<Integer, Long,String,String> map(Model value) throws Exception {
            return new Tuple4<Integer,Long,String,String>(value.getObjtype(),1L,value.getColor1(),value.getColor2());
        }
    }
}
