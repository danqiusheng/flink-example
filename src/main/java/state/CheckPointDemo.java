package state;


import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import streaming.model.StaEntity;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 容错机制示例
 * target:定时checkpoint，在第五个窗口抛出异常，看是否正常恢复.保证exactly-one
 * input:从kafka读取消息
 * output:输出窗口数字和为偶数的数据
 * 使用类{@link sink.WriteToKafkaDemo}
 */
public class CheckPointDemo {
    static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    static boolean flag = true;
    static int counter = 0;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);//1s 开始一个checkpoint
        // 设置模式为 exactly-one
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置两个checkpoint之间的时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // checkpoints  必须在一分钟内完成，否则就会被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间，只有一个检查点正在进行中
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

//        env.setStateBackend(new FsStateBackend("file:///e:/flink/checkpoindemo"));
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.6.28:9092");
        properties.setProperty("group.id", "myState");// 消费者的分组id
        properties.setProperty("auto.offset.reset", "earliest"); // 总是从开头读取消息

        DataStream<Tuple3<String, Long, Long>> result = env.addSource(new FlinkKafkaConsumer010<>("myState",//
                new TypeInformationSerializationSchema<>(TypeInformation.of(StaEntity.class), env.getConfig()), properties))//new MySourceFunction()) //
                .assignTimestampsAndWatermarks(new MyWatermark())//
                .keyBy("code")//
                .timeWindow(Time.seconds(3))//
                .apply(new MyWindow());

        Pattern<Tuple3<String, Long, Long>, ?> warningPattern = Pattern.<Tuple3<String, Long, Long>>begin("first")//
                .where(new MyFilterFunction())//
                .within(Time.seconds(3));

        // 选择数据
        // 这里加入CEP，当窗口内数据和为偶数的时候打印
        CEP.pattern(result, warningPattern)
                .select(new MyPatternSelectFunction())
                .print();
        env.execute("execute checkpoint");
    }


    private static class MyPatternSelectFunction implements PatternSelectFunction<Tuple3<String, Long, Long>, StaEntity> {
        @Override
        public StaEntity select(Map<String, List<Tuple3<String, Long, Long>>> pattern) throws Exception {
            Tuple3<String, Long, Long> value = pattern.get("first").get(0);
            return new StaEntity("total", value.f1);
        }
    }

    // 规则
    private static class MyFilterFunction extends IterativeCondition<Tuple3<String, Long, Long>> {
        @Override
        public boolean filter(Tuple3<String, Long, Long> value, Context<Tuple3<String, Long, Long>> ctx) throws Exception {
            if (value.f1 % 2 == 0) {
                return true;
            }
            return false;
        }
    }

    // 自定义窗口方法
    public static class MyWindow implements WindowFunction<StaEntity, Tuple3<String, Long, Long>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow window, Iterable<StaEntity> input, Collector<Tuple3<String, Long, Long>> out) throws Exception {
            long sum = 0;
            long count = 0;
            System.out.println("---------------------");
            StaEntity m = new StaEntity();
            for (Iterator<StaEntity> iterator = input.iterator(); iterator.hasNext(); ) {
                count++;
                StaEntity model = iterator.next();
                System.out.println(model);
                sum += model.getData();
                m = model;
            }

            counter++;

            if (counter == 5) {
                flag = false;
                counter++;
                throw new Exception("测试容错..");
            }

            out.collect(new Tuple3<String, Long, Long>(m.getCode(), sum, count));
            System.out.println("-------------------");
            System.out.println("window start:" + format.format(window.getStart()) + ";window end:" + format.format(window.getEnd()));
        }
    }

    // 自定义生成水印
    public static class MyWatermark implements AssignerWithPeriodicWatermarks<StaEntity> {
        @Nullable
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis() - 1000);
        }
        public long extractTimestamp(StaEntity element, long previousElementTimestamp) {
            return element.getCreateTime();
        }
    }
}
