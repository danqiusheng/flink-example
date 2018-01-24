package table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import table.util.RandomGenerator;

/**
 * target:table inner join （Time-windowed Join）
 * student (name,classid,,age),(classid,classname)
 * TODO:有错误，已修复。
 * 问题：join是基于某个时间属性进行join操作,误操作为单个字段进行join
 * 因为进行流连接有时间差，通过时间字段进行控制。
 */
public class TableTimeWindowJoinDemo {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为流查询创建表环境
        StreamTableEnvironment streamTableEnvironment = TableEnvironment.getTableEnvironment(env);
        // 获取班级数据
        // declare an additional logical field as a processing time attribute
        Table right = streamTableEnvironment.fromDataStream(env.fromCollection(RandomGenerator.generatorClasses()), "id,className, cTimestamp, ltime.proctime"); // 新增一个时间字段
        // 获取教师数据
        Table left = streamTableEnvironment.fromDataStream(env.fromCollection(RandomGenerator.generatorTeacher()), "teacherId,name,classId,tTimestamp,rtime.proctime");

        // 数据进行连接
        Table table = left.join(right)
                .where("classId = id  && rtime >= ltime - 2.seconds && rtime < ltime + 3.seconds") //
                .select("name,className");
        TupleTypeInfo<Tuple2<String, String>> tupleType = new TupleTypeInfo<>(
                Types.STRING(),
                Types.STRING());

        streamTableEnvironment.toAppendStream(table, tupleType).print();
        env.execute("table time window join demo");
    }
}
