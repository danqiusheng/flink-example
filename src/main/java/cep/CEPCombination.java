package cep;


import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streaming.model.StaEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 示例：
 * 如何使用,暂时未理解其中如何执行，结果为什么是这样
 * TODO:暂未明白
 */
public class CEPCombination {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StaEntity> result = env.fromCollection(generatorSource());

        Pattern<StaEntity, ?> pattern = Pattern.<StaEntity>begin("first").next("middle").where(new SimpleCondition<StaEntity>() {
            @Override
            public boolean filter(StaEntity staEntity) throws Exception {
                return staEntity.getCode().equals("0001");
            }
        }).followedBy("end").where(new SimpleCondition<StaEntity>() {
            @Override
            public boolean filter(StaEntity staEntity) throws Exception {
                return staEntity.getData() >= 50;
            }
        });

        CEP.pattern(result, pattern).select(new PatternSelectFunction<StaEntity, StaEntity>() {
            @Override
            public StaEntity select(Map<String, List<StaEntity>> map) throws Exception {
                System.out.println(map);
                return map.get("end").get(0);
            }
        }).print();

        env.execute("CEPWhere");
    }

    // 生成数据
    public static List<StaEntity> generatorSource() {
        System.out.println("======原始数据开始=========");
        // 写入数据
        List<StaEntity> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Double d = Math.random() * 100;
            StaEntity staEntity;
            if (i % 2 == 0) {
                staEntity = new StaEntity("0001", d.longValue(), d.longValue());
            } else
                staEntity = new StaEntity("0002", d.longValue(), d.longValue());
            System.out.println(staEntity);
            data.add(staEntity);
        }
        System.out.println("======原始数据结束=========");
        return data;
    }
}
