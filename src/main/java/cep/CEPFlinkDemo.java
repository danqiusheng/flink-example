package cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/6/11
 * 示例demo：1,2,3,4,1,2,3
 *
 * @author danqiusheng
 */
public class CEPFlinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //              0     1   2   3    4     5    6    7    8    9
        String[] arr = {"1", "2", "2", "3", "1", "2", "5", "1", "4", "5", "2", "1", "6"};


        List<Student> list = new ArrayList<>();


        for (int i = 0; i < arr.length; i++) {
            Student student = new Student(arr[i], "Student_" + i);
            list.add(student);
        }


        DataStreamSource<Student> dataStreamSource = env.fromCollection(list);


        //--------强制必须连续，开始
//        Pattern<Student, Student> studentPattern = Pattern.<Student>begin("start").where(new SimpleCondition<Student>() {
//            @Override
//            public boolean filter(Student value) throws Exception {
//                return value.getId().equals("1");
//            }
//        }).next("middle").where(new SimpleCondition<Student>() {
//            @Override
//            public boolean filter(Student value) throws Exception {
//                return value.getId().equals("2");
//            }
//        });  // 12 必须连续在一起,输出结果有两条
        //--------强制必须连续，结束


        //----------非必须连续开始------------
//        Pattern<Student, Student> studentPattern = Pattern.<Student>begin("start").where(new SimpleCondition<Student>() {
//            @Override
//            public boolean filter(Student value) throws Exception {
//                return value.getId().equals("1");
//            }
//        }).followedBy("middle").where(new SimpleCondition<Student>() {
//            @Override
//            public boolean filter(Student value) throws Exception {
//                return value.getId().equals("2");
//            }
//        }).within(Time.seconds(1));  // 12 非必须连续在一起 输出结果有三条
        //----------非必须连续结束------------


        //----------- 开始 --------------
//        Pattern<Student, Student> studentPattern = Pattern.<Student>begin("start").where(new SimpleCondition<Student>() {
//            @Override
//            public boolean filter(Student value) throws Exception {
//                return value.getId().equals("1");
//            }
//        }).next("middle").where(new SimpleCondition<Student>() {
//            @Override
//            public boolean filter(Student value) throws Exception {
//                return value.getId().equals("2");
//            }
//        }).times(2).within(Time.seconds(1));  // 1开始2不连续但必须2次  非强制要求2必须连续在一起
        //-----------结束--------------

        //----------开始---------------
        Pattern<Student, Student> studentPattern = Pattern.<Student>begin("start").where(new SimpleCondition<Student>() {
            @Override
            public boolean filter(Student value) throws Exception {
                return value.getId().equals("1");
            }
        }).next("middle").where(new SimpleCondition<Student>() {
            @Override
            public boolean filter(Student value) throws Exception {
                return value.getId().equals("2");
            }
        }).times(2).consecutive().within(Time.seconds(1));  // 1开始2必须连续且必须2次
        //---------结束--------------------

        PatternStream<Student> patternStream = CEP.pattern(dataStreamSource, studentPattern);


        patternStream.select(new PatternSelectFunction<Student, Student>() {
            @Override
            public Student select(Map<String, List<Student>> pattern) throws Exception {
                System.out.println("test the demo");
                System.out.println(pattern);
                return pattern.get("start").get(0);
            }
        }).print();


        env.execute("the cep demo by the test");
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
