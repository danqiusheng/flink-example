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
 * where和or拼接
 * target:熟悉or,和where 条件成 or拼接;
 * 比如 a or b 只要其中一个成立即可
 * output:输出code为0001或者data大于50
 */
public class CEPWhereAndOr {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StaEntity> source = env.fromCollection(CEPCombination.generatorSource());
        Pattern<StaEntity, ?> pattern = Pattern.<StaEntity>begin("first").where(new MySimpleCondition()).or(new MyThanSimpleCondition());
        CEP.pattern(source, pattern).select(new MyPatternSelectFunction()).print();
        env.execute("cep where and or ");
    }

    // 大于50
    private static class MyThanSimpleCondition extends SimpleCondition<StaEntity> {
        @Override
        public boolean filter(StaEntity staEntity) throws Exception {
            return staEntity.getData() >= 50;
        }
    }

    // 挑选结果
    private static class MyPatternSelectFunction implements PatternSelectFunction<StaEntity, StaEntity> {
        @Override
        public StaEntity select(Map<String, List<StaEntity>> map) throws Exception {
            System.out.println(map);
            return map.get("first").get(0);
        }
    }


    // 简单条件
    private static class MySimpleCondition extends SimpleCondition<StaEntity> {

        @Override
        public boolean filter(StaEntity staEntity) throws Exception {
            // System.out.println(staEntity);
            return staEntity.getCode().equals("0001");
        }
    }
}
