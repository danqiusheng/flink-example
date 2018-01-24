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
 * where示例 ,多个where表现为AND拼接
 * targetL:输出code是001 并且 data 大于50
 */
public class CEPWhere {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StaEntity> source = env.fromCollection(CEPCombination.generatorSource());
        Pattern<StaEntity, ?> pattern = Pattern.<StaEntity>begin("first").where(new MySimpleCondition()).where(new MyThanPatternSelectFunction());
        CEP.pattern(source, pattern).select(new MyPatternSelectFunction()).print();
        env.execute("cep demo 2");
    }

    private static class MyThanPatternSelectFunction extends SimpleCondition<StaEntity> {
        @Override
        public boolean filter(StaEntity staEntity) throws Exception {
            return staEntity.getData() > 50;
        }
    }

    private static class MyPatternSelectFunction implements PatternSelectFunction<StaEntity, StaEntity> {
        @Override
        public StaEntity select(Map<String, List<StaEntity>> map) throws Exception {
            return map.get("first").get(0);
        }
    }

    private static class MySimpleCondition extends SimpleCondition<StaEntity> {

        @Override
        public boolean filter(StaEntity staEntity) throws Exception {
            // System.out.println(staEntity);
            return staEntity.getCode().equals("0001");
        }
    }
}
