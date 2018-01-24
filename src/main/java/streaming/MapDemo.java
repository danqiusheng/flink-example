package streaming;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将一个元素转换另一个元素
 * MapFunction<T,O>
 * 泛型T：代表输入的类型
 * 泛型O：代表输出的类型
 * <p>
 * transformation :Map
 * target:将数字转换为 "Hello" + 数字
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        // 得到执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> data = env.generateSequence(10, 1000);

        DataStream<String> la = data.map((val) -> "Hello"+ val.toString());
        // 打印控制台
        la.print();
        // 执行Job
        env.execute();
    }
}
