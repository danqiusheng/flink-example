package multi;


import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import streaming.model.StaEntity;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * CoGroup 进行流之间的连接
 * target: 窗口类的数据进行排序
 */
public class CoGroupSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 源1
        List<StaEntity> data = gerneratorSource();
        DataStream<StaEntity> source1 = env.fromCollection(data);
        DataStream<StaEntity> source1_1 = source1.assignTimestampsAndWatermarks(new MyWatermark());

        // 源2
        List<StaEntity> dataList = new ArrayList<StaEntity>();
        for (StaEntity staEntity : data) {
            if (staEntity.getData() % 2 == 0) {
                dataList.add(new StaEntity("0001", staEntity.getCreateTime(), staEntity.getData()));
            } else {
                dataList.add(new StaEntity("0002", staEntity.getCreateTime(), staEntity.getData()));
            }
        }

        DataStream<StaEntity> source2 = env.fromCollection(dataList);
        DataStream<StaEntity> source2_1 = source2.assignTimestampsAndWatermarks(new MyWatermark());

        // 不同类型可以进行join 操作
        source1_1.coGroup(source2_1)
                .where(new MyKeySelector())
                .equalTo(new MyKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new MyCoGroupFunction())
                .keyBy(0)
                .sum(1)
                .rebalance()
                .print();

        env.execute();
    }

    private static List<StaEntity> gerneratorSource() {
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

    private static  class MyWatermark implements AssignerWithPeriodicWatermarks<StaEntity> {
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis());
        }

        @Override
        public long extractTimestamp(StaEntity element, long previousElementTimestamp) {
            return element.getCreateTime();
        }
    }

    private static  class MyKeySelector implements KeySelector<StaEntity, String>{
        @Override
        public String getKey(StaEntity value) throws Exception {
            // System.out.println(value);
            return value.getCode();
        }
    }


    private static class MyCoGroupFunction implements CoGroupFunction<StaEntity, StaEntity, Tuple2<String, Long>> {
        @Override
        public void coGroup(Iterable<StaEntity> first, Iterable<StaEntity> second, Collector<Tuple2<String, Long>> out) throws Exception {
            List<StaEntity> list = new ArrayList<StaEntity>();
            String code = "";
            System.out.println("------one-----------------");
            for (Iterator<StaEntity> iterator = first.iterator(); iterator.hasNext(); ) {
                StaEntity StaEntity = iterator.next();
                list.add(StaEntity);
                System.out.println(StaEntity);
                code = StaEntity.getCode();
            }

            System.out.println("------two-----------------");
            List<StaEntity> list2 = new ArrayList<StaEntity>();
            for (Iterator<StaEntity> iterator = second.iterator(); iterator.hasNext(); ) {
                StaEntity myStaEntity = iterator.next();
                list.add(myStaEntity);
                System.out.println(myStaEntity);
            }

            // 对结果进行排序
            System.out.println("排序后....");
            list.sort(new Comparator<StaEntity>() {
                @Override
                public int compare(StaEntity o1, StaEntity o2) {
                    return new Long(o1.getData() - o2.getData()).intValue();
                }
            });

            for (streaming.model.StaEntity StaEntity : list) {
                System.out.println(StaEntity);
                out.collect(new Tuple2<>(StaEntity.getCode(), StaEntity.getData()));
            }


        }
    }
}
