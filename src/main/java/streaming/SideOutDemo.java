package streaming;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * target:熟悉side out
 * 只能在以下方法收集数据：
 * ProcessFunction
 * CoProcessFunction
 * ProcessWindowFunction
 * ProcessAllWindowFunction
 */
public class SideOutDemo {
    public static void main(String[] args) throws Exception {
        // this needs to be an anonymous inner class, so that we can analyze the type
        OutputTag<String> outputTag = new OutputTag<String>("side-output") {
        };

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> input = env.fromCollection(generatorSource());
        SingleOutputStreamOperator<Integer> mainDataStream = input
                .process(new ProcessFunction<Integer, Integer>() {
                    @Override
                    public void processElement(
                            Integer value,
                            Context ctx,
                            Collector<Integer> out) throws Exception {
                        out.collect(value);
                        if (value > 1000) {
                            // emit data to side output
                            ctx.output(outputTag, "sideout-" + String.valueOf(value));
                        }
                    }
                });


        DataStream<String> sideOutputStream = mainDataStream.getSideOutput(outputTag);
        sideOutputStream.print();
        mainDataStream.print();

        env.execute("side out demo");
    }

    // 生成数据
    public static List<Integer> generatorSource() {
        // 写入数据
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Double d = Math.random() * 10000;
            data.add(d.intValue());
        }
        return data;
    }
}
