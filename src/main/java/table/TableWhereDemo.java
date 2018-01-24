package table;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import streaming.model.StaEntity;
import table.util.RandomGenerator;

/**
 * target:where如何使用 以及filter  获取数据大于50并且code等于0001
 */
public class TableWhereDemo {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为流查询创建表环境
        StreamTableEnvironment streamTableEnvironment = TableEnvironment.getTableEnvironment(env);
        Table table = streamTableEnvironment.fromDataStream(env.fromCollection(RandomGenerator.generatorSource()));

        // 或者其他的方式 比如filter
        Table result =table.filter("data>50").filter("code ==='0001'");
       // Table result = table.where("data > 50").where("code ==='0001' ");

        streamTableEnvironment.toAppendStream(result, StaEntity.class).print();

        env.execute("table where demo");
    }
}