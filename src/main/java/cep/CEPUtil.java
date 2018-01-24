package cep;


import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streaming.model.StaEntity;

import java.util.List;
import java.util.Map;


/**
 * 指定一个停止的条件
 * target:熟悉until
 * TODO:暂未明白
 */
public class CEPUtil {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StaEntity> source = env.fromCollection(CEPCombination.generatorSource());
        Pattern<StaEntity, ?> pattern = Pattern.<StaEntity>begin("first").oneOrMore().allowCombinations().until(new SimpleCondition<StaEntity>() {
            @Override
            public boolean filter(StaEntity staEntity) throws Exception {
                return staEntity.getData() > 50;
            }
        });
        CEP.pattern(source, pattern).select(new MyPatternSelectFunction()).print();
        env.execute("cep until");
    }


    // 挑选结果
    private static class MyPatternSelectFunction implements PatternSelectFunction<StaEntity, StaEntity> {
        @Override
        public StaEntity select(Map<String, List<StaEntity>> map) throws Exception {
            System.out.println(map);
            return map.get("first").get(0);
        }
    }

}
