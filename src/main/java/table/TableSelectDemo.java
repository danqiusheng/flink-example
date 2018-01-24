package table;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import streaming.model.StaEntity;
import table.util.RandomGenerator;


/**
 * target:select属性字段
 */
public class TableSelectDemo {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为流查询创建表环境
        StreamTableEnvironment streamTableEnvironment = TableEnvironment.getTableEnvironment(env);
        streamTableEnvironment.registerDataStream("staEntity", env.fromCollection(RandomGenerator.generatorSource()));


        Table table= streamTableEnvironment.scan("staEntity").select("code , createTime  , data");

        DataStream<StaEntity> dataStream = streamTableEnvironment.toAppendStream(table, StaEntity.class);

        dataStream.print();

        env.execute("table select demo");
    }
}
