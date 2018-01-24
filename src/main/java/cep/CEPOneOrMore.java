package cep;


import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streaming.model.StaEntity;

import java.util.List;
import java.util.Map;

/**
 * oneOrMore：使用relaxed internal contiguity
 * target: 多次出现事件
 */
public class CEPOneOrMore {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StaEntity> source = env.fromCollection(CEPCombination.generatorSource());
        Pattern<StaEntity, ?> pattern = Pattern.<StaEntity>begin("first").oneOrMore();
        CEP.pattern(source,pattern).select(new MyPatternSelectFunction()).print();
        env.execute("cep oneOrMore");
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
