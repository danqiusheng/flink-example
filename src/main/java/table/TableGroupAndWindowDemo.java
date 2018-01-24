package table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import streaming.model.StaEntity;
import table.util.RandomGenerator;

/**
 * table api groupby window aggregation 示例
 * target:窗口聚合总和
 * TODO:有错误，需修改。 问题：
 */
public class TableGroupAndWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment streamTableEnvironment = TableEnvironment.getTableEnvironment(env);


        DataStream<StaEntity> source = env.fromCollection(RandomGenerator.generatorSource()).assignTimestampsAndWatermarks(new RandomGenerator.MyWatermark());
        // 获取表，default table name
        //Table table = streamTableEnvironment.fromDataStream(source);

        streamTableEnvironment.registerDataStream("staEntity", source, "code,data,createTime.rowtime"); //a,b,c,rowtime
        Table result = streamTableEnvironment.scan("staEntity").window(Tumble.over("5.minutes").on("createTime").as("w"))
                .groupBy("code,w")//.table();// group by key and window
                .select("code,data.sum");

        TupleTypeInfo<Tuple2<String, Long>> tupleType = new TupleTypeInfo<>(
                Types.STRING(),
                Types.LONG());

        DataStream<Tuple2<Boolean, Tuple2<String, Long>>> resultStream = streamTableEnvironment.toRetractStream(result, tupleType);
        resultStream.print();
        env.execute("table group and window demo");
    }


}
