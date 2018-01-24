package table;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import streaming.model.StaEntity;

import java.util.ArrayList;
import java.util.List;

/**
 * 注册表，并在表中查询。
 * target:随机生成数,根据code分组并求和data
 * 输出：（boolean,code,sum） --boolean 代表该列是否被更新 true为更新，false为不更新
 */
public class TableGroupDemo {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为流查询创建表环境
        StreamTableEnvironment streamTableEnvironment = TableEnvironment.getTableEnvironment(env);

        streamTableEnvironment.registerDataStream("staEntity", env.fromCollection(generatorSource()));

        Table result = streamTableEnvironment.scan("staEntity").groupBy("code").select("code, data.sum");
        // 将表转换为追加的流对于每一行
        // 将查询结果转换为流并打印
        // 两个模式：append 追加模式，retract 撤销模式
        DataStream<Tuple2<Boolean, Row>> dsRow = streamTableEnvironment.toRetractStream(result, Row.class);
        // 打印结果
        dsRow.print();
        env.execute("table query demo ");
    }

    private static List<StaEntity> generatorSource() {
        List<StaEntity> data = new ArrayList<>();
        long sum1 = 0;
        long sum2 = 0;
        for (int i = 0; i < 10; i++) {
            Double d = Math.random() * 100;
            StaEntity staEntity = null;

            if (i % 2 == 0) {
                sum1 += d.longValue();
                staEntity = new StaEntity("0001", (System.currentTimeMillis() + 1000 * d.intValue()), d.longValue());
            } else {
                sum2 += d.longValue();
                staEntity = new StaEntity("0002", (System.currentTimeMillis() + 1000 * d.intValue()), d.longValue());
            }
            data.add(staEntity);
        }
        System.out.println("sum1=" + sum1 + "; sum2=" + sum2);
        return data;
    }
}
