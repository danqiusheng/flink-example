package table;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import streaming.model.StaEntity;

import java.util.ArrayList;
import java.util.List;

/**
 * target:创建TableEnviroment
 */
public class TableEnvironmentDemo {
    public static void main(String[] args) {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为流查询创建表环境
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        // 批环境
        //ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        // StreamTableEnvironment sTableEnv = TableEnvironment.getTableEnvironment(environment);

        // 注册表
       // tableEnv.registerDataStreamInternal("staEntity",env.fromCollection(generatorSource()));

    }


    private static List<StaEntity> generatorSource() {
        List<StaEntity> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Double d = Math.random() * 100;
            data.add(new StaEntity("0001", (System.currentTimeMillis() + 1000 * d.intValue()), d.longValue()));
        }
        return data;
    }
}
