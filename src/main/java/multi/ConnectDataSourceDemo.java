package multi;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import streaming.model.MyModel;
import streaming.model.StaEntity;

import java.util.ArrayList;
import java.util.List;

/**
 * connect 连接DataSource
 * target:键控分区，并共享状态
 */
public class ConnectDataSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 源1
        List<StaEntity> data = generatorSource();
        DataStream<StaEntity> source1_1 = env.fromCollection(data).assignTimestampsAndWatermarks(new MySource1Watermark());
        // 源2
        DataStream<MyModel> source2_1 = env.fromCollection(generatorMySource(data)).assignTimestampsAndWatermarks(new MySource2Watermark());
        source1_1.keyBy("code")
                .connect(source2_1.keyBy("code"))
                .process(new MyProcessFunction())
                .rebalance()
                .print();
        env.execute("ConnectDataSourceDemo");
    }


    private static class MySource2Watermark implements AssignerWithPeriodicWatermarks<MyModel> {
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis() - 10);
        }

        public long extractTimestamp(MyModel element, long previousElementTimestamp) {
            return element.getCreateTime();
        }
    }

    private static class MySource1Watermark implements AssignerWithPeriodicWatermarks<StaEntity> {
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis() - 10);
        }

        public long extractTimestamp(StaEntity element, long previousElementTimestamp) {
            return element.getCreateTime();
        }
    }


    private static List<MyModel> generatorMySource(List<StaEntity> data) {
        List<MyModel> dataList = new ArrayList<MyModel>();
        for (StaEntity StaEntity : data) {
            if (StaEntity.getData() % 2 == 0) {
                dataList.add(new MyModel("0001", StaEntity.getCreateTime(), StaEntity.getData()));
            } else {
                dataList.add(new MyModel("0002", StaEntity.getCreateTime(), StaEntity.getData()));
            }
        }
        return dataList;
    }

    private static List<StaEntity> generatorSource() {
        List<StaEntity> data = new ArrayList<StaEntity>();
        for (int i = 0; i < 15; i++) {
            Double d = Math.random() * 100;
            if (i % 2 == 0) {
                data.add(new StaEntity("0001", (System.currentTimeMillis() + 1000 * i), d.longValue()));
            } else
                data.add(new StaEntity("0002", (System.currentTimeMillis() + 1000 * i), d.longValue()));

        }
        return data;
    }


}

class MyProcessFunction extends CoProcessFunction<StaEntity, MyModel, Tuple2<String, Long>> {

    private transient MapState<String, Long> mapState;
    private transient ReducingState<Tuple2<String, Long>> reduceState;

    @Override
    public void processElement1(StaEntity value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        Long count = mapState.get(value.getCode());
        int sum = 0;
        if (count == null) {
            mapState.put(value.getCode(), 1L);
        } else {
            mapState.put(value.getCode(), count + 1L);
        }
        System.out.println(mapState.get("0002") + "----" + mapState.get("0001"));
        for (String key : mapState.keys()) {
            sum += mapState.get(key);
        }
        if (sum > 10) {// 如果大于3，发出警告
            for (String key : mapState.keys()) {
                System.out.println("key:" + key + "; value:" + mapState.get(key));
                out.collect(new Tuple2<>(key, mapState.get(key)));
            }
        }
        // ctx.timerService().registerEventTimeTimer(value.getCreateTime());
    }

    @Override
    public void processElement2(MyModel value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        Long count = mapState.get(value.getCode());
        if (count == null) {
            mapState.put(value.getCode(), 1L);
        } else {
            mapState.put(value.getCode(), count + 1L);
        }

        int sum = 0;
        System.out.println(mapState.get("0002") + "----" + mapState.get("0001"));
        for (String key : mapState.keys()) {
            sum += mapState.get(key);
        }
        if (sum > 10) {// 如果大于10， 发出信息
            for (String key : mapState.keys()) {
                out.collect(new Tuple2<>(key, mapState.get(key)));
            }
        }
        //  ctx.timerService().registerEventTimeTimer(value.getCreateTime());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<String, Long>("the code count", String.class, Long.class);
        // 初始化状态
        ReducingStateDescriptor<Tuple2<String, Long>> reducingStateDescriptor = new ReducingStateDescriptor<Tuple2<String, Long>>(
                "test", new MyReduceFunction(), TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
        }));
        mapState = getRuntimeContext().getMapState(descriptor);
        reduceState = getRuntimeContext().getReducingState(reducingStateDescriptor);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        System.out.println("on timer...");
        Iterable<String> iterable = mapState.keys();
        for (String key : iterable) {
            System.out.println("key:" + key + "; value:" + mapState.get(key));
            out.collect(new Tuple2<>(key, mapState.get(key)));
        }
        mapState.clear();
        System.out.println("timer over....");
    }

    @Override
    public void close() throws Exception {
    }
}


class MyReduceFunction implements ReduceFunction<Tuple2<String, Long>> {

    @Override
    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
    }
}