package streaming;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streaming.model.Student;

/**
 * 按键分组
 * target:根据班级分组，求班级总人数
 */
public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Student,Integer>> resource = env.fromElements(new Tuple2<>(new Student("x","1",12),1),
                new Tuple2<>(new Student("x","2",12),1),
                new Tuple2<>(new Student("x","2",12),1),
                new Tuple2<>(new Student("x","2",12),1),
                new Tuple2<>(new Student("x","1",12),1),
                new Tuple2<>(new Student("x","2",12),1),
                new Tuple2<>(new Student("x","1",12),1));

       DataStream<Tuple2<Student,Integer>>  data = resource.keyBy("f0.classes").sum(1);
            data.print() ;
            env.execute();
    }
}
