package async;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import streaming.model.StaEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 异步请求示例
 * target:理解异步是什么，在Flink中如何使用
 */
public class AsyncDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StaEntity> source = env.fromCollection(generatorSource());
        AsyncDataStream.orderedWait(source, new MyAsyncFunction(), 3, TimeUnit.SECONDS);

        env.execute("async demo ...");
    }

    private static class MyAsyncFunction extends RichAsyncFunction<StaEntity, Tuple2<String, Long>> {
        @Override
        // 异步调用该结果
        public void asyncInvoke(StaEntity input, ResultFuture<Tuple2<String, Long>> resultFuture) throws Exception {

            System.out.println("deng...");
        }

        @Override
        // 初始化连接
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        // 关闭资源
        public void close() throws Exception {
            super.close();
        }
    }

    // 生成数据,
    public static List<StaEntity> generatorSource() {
        // 写入数据
        List<StaEntity> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Double d = Math.random() * 10000;
            data.add(new StaEntity("0001", d.longValue(), d.longValue()));
        }
        return data;
    }

}
