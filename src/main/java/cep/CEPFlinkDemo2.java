package cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/6/13.
 */
public class CEPFlinkDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //              0     1   2   3     4     5    6    7
        String[] arr = {"c", "d", "a", "a", "a", "d", "a", "b"};


        List<Student> list = new ArrayList<>();


        for (int i = 0; i < arr.length; i++) {
            Student student = new Student(arr[i], "Student_" + i);
            list.add(student);
        }


        DataStreamSource<Student> dataStreamSource = env.fromCollection(list);

        Pattern<Student, Student> studentPattern =   Pattern.<Student>begin("start").where(new SimpleCondition<Student>() {
            @Override
            public boolean filter(Student value) throws Exception {
                return value.getId().equals("c");
            }
        })  .followedBy("middle").where(new SimpleCondition<Student>() {
            @Override
            public boolean filter(Student value) throws Exception {
                return value.getId().equals("a");
            }
        }).oneOrMore().consecutive()
                .followedBy("end1").where(new SimpleCondition<Student>() {
            @Override
            public boolean filter(Student value) throws Exception {
                return value.getId().equals("b");
            }
        });
        PatternStream<Student> patternStream = CEP.pattern(dataStreamSource, studentPattern);

        patternStream.select(new PatternSelectFunction<Student, Student>() {
            @Override
            public Student select(Map<String, List<Student>> pattern) throws Exception {
                System.out.println("test the demo");
                System.out.println(pattern);
                return pattern.get("start").get(0);
            }
        }).print();


        env.execute("the cep demo2 by the test ");
    }

    static class Student {
        private String id;

        private String name;

        public Student() {

        }

        public Student(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
