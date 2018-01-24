package training;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 *  维基百科计算每个用户在给定事件窗口内编辑的字节数
 *   后期修改，将结果写入到kafka中
 * @author  丹丘生
 */
public class WikiAnakysis {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<WikipediaEditEvent> resources = env.addSource(new WikipediaEditsSource());

       /* resources.keyBy(new KeySelector<WikipediaEditEvent, String>() {

            @Override
            public String getKey(WikipediaEditEvent value) throws Exception {
                return value.getUser();
            }
        });*/

     DataStream<Tuple2<String,Long>>  result =  resources.keyBy((value) ->
             value.getUser())//
             .timeWindow(Time.seconds(5))//
             .aggregate(new MyAggregate());
    result.print();
    env.execute(" wiki anakysis");
    }
}

class MyAggregate implements AggregateFunction<WikipediaEditEvent,Tuple2<String,Long> ,Tuple2<String,Long>> {


    // 初始化累加器Creates a new accumulator, starting a new aggregate.
    @Override
    public Tuple2<String, Long> createAccumulator() {
        return new Tuple2<>("",0L);
    }

    // 每进入一个元素，累加器对其综合
    @Override
    public Tuple2<String, Long> add(WikipediaEditEvent value, Tuple2<String, Long> accumulator) {
            accumulator.f0 = value.getUser();
            // 得到编辑的个数
            accumulator.f1 += value.getByteDiff();
            return accumulator;
    }

    @Override
    public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
        return accumulator;
    }


    //这个函数可以重用任何一个给定的累计器，作为合并的目标，并返回它。
    // 假设在传递给这个函数之后，给定的累计器将不再被使用。
    @Override
    public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
        a.f0 = b.f0;
        a.f1 += b.f1;
        return a;
    }
}