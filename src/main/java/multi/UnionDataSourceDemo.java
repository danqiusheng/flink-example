package multi;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import streaming.model.StaEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 采用union将流进行连接，可多个,但类型必须一致
 * target:合成新流并进行统计数据大于5000 采用CEP
 */
public class UnionDataSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StaEntity> source1 = env.fromCollection(generatorSource());
        DataStream<StaEntity> source2 = env.fromCollection(generatorSource());
        DataStream<StaEntity> result = source1.union(source2);
        Pattern<StaEntity, ?> warningPattern = Pattern.<StaEntity>begin("first")//
                .subtype(StaEntity.class)//
                .where(new FilterThanFunction())//
                .within(Time.seconds(10)); //在10s之内的

        PatternStream<StaEntity> patternStream = CEP.pattern(result, warningPattern);
        patternStream.select(new MyPatternSelectFunction())
                .print();
        env.execute("UnionDataSourceDemo");
    }


    private static class  MyPatternSelectFunction implements PatternSelectFunction<StaEntity,Tuple2<String,Long>> {
        @Override
        public Tuple2<String, Long> select(Map<String, List<StaEntity>> map) throws Exception {
             StaEntity staEntity =  map.get("first").get(0);
            return new Tuple2<>(staEntity.getCode(),staEntity.getData());
        }
    }

    private static class FilterThanFunction extends IterativeCondition<StaEntity> {
        @Override
        public boolean filter(StaEntity staEntity, Context<StaEntity> context) throws Exception {
            if (staEntity.getData() > 5000)
                return true;
            return false;
        }
    }

    private static List<StaEntity> generatorSource() {
        List<StaEntity> data = new ArrayList<StaEntity>();
        for (int i = 0; i < 15; i++) {
            Double d = Math.random() * 10000;
            if (i % 2 == 0)
                data.add(new StaEntity("0001", (System.currentTimeMillis() + 1000 * d.intValue()), d.longValue()));
            else
                data.add(new StaEntity("0002", (System.currentTimeMillis() + 1000 * d.intValue()), d.longValue()));
        }
        return data;
    }
}
