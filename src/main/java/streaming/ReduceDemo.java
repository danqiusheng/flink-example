package streaming;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streaming.model.Student;

/**
 * Reduce
 * 将一组元素组合成一个元素通过重复组合两个元素成一个元素。Reduce可以应用于完整的数据集或分组的数据集
 * target:根据班级分组，reduce各班级学生的年龄之和
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Student> resources = env.fromElements(
                new Student("x", "1", 23),
                new Student("x", "1", 73),
                new Student("x", "1", 53),
                new Student("x", "1", 43),
                new Student("x", "4", 3),
                new Student("x", "5", 13),
                new Student("x", "6", 23));
        DataStream<Student> data = resources
                .keyBy("classes")
                .reduce((value1, value2) -> new Student("这是新增的", value1.getClasses(), value1.getAge() + value2.getAge()));
        data.print();
        env.execute("reduce demo");
    }
}
