package cep.demo;

import cep.demo.model.People;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 创建预警并将数据写入到内存数据库(人流预警demo）
 * Created by Administrator on 2018/5/15.
 * 流程： 与流里面的数据进行汇总并进行匹配预警，查到与之匹配的数据则发出预警。
 * 过程：模拟数据，与内存数据里面数据进行匹配，满足则发出预警（写入到其他数据库或者kafka。
 *
 * @author danqiusheng
 */
public class CEPDemo_1 {
    public static void main(String[] args) throws Exception {
        // 获取执行环境， 本地执行 采用local环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 模拟单流汇聚，然后查询数据库进行匹配
        DataStreamSource<Long> dataStream = env.generateSequence(10, 2000);

        DataStream<People> result = dataStream.map((id) -> { // 模拟数据进行转换成对象进行匹配（模拟数据）
            return new People(id + "", "test_" + id, id);
        })// 进行拦截过滤，如果没有匹配的则让其不通过，不做任何操作;
                .filter(new RichFilterFunction<People>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 加载数据库连接
                        System.out.println("加载数据库连接中......");
                    }
                    @Override
                    public void close() throws Exception {
                        System.out.println("关闭数据库连接中......");
                    }
                    @Override
                    public boolean filter(People people) throws Exception {
                        // 查询数据库，有没有匹配的人员
                        //String sql = "select * from xx ";
                        // 根据结果去判断是否有匹配的人员
                        if (people.getId().equals("23")) {
                            return true;
                        }
                        return false;
                    }
                });


        // 查到匹配的数据，将数据写入到其他地方中
        // 这里模拟 打印控制台
        result.print();
        //
        //        dataStream.addSink(new SinkFunction<People>() {
        //            @Override
        //            public void invoke(People value) throws Exception {
        //
        //            }
        //        });


        env.execute("test the cep demo 1");
    }
}
