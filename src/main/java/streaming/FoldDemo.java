package streaming;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streaming.model.Student;


/**
 * fold the value  has be deprecated
 * use {@link AggregateFunction} instead
 * target:分班级求班级中年纪最大的同学
 */
public class FoldDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Student> resources =  env.fromElements(
                new Student("x","1",23),
                new Student("x","1",73),
                new Student("x","2",53),
                new Student("y","2",43),
                new Student("y","1",3),
                new Student("x","2",13),
                new Student("x","2",23));
        DataStream<Student> result  = resources.keyBy("classes").max("age");
        result.print();
        env.execute();

    }
}
