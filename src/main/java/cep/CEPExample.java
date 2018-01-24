package cep;

import cep.pojo.TemperatureAlert;
import cep.pojo.TemperatureEvent;
import cep.pojo.TemperatureWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * target:监控温度事件，当检测到连续两个超过阀值的温度，即生成一个当前平均温度的警告。
 * 如果看到两个连续的升温警告，则报警（alert）
 * 思路：当前找到超过阀值的，则匹配下一个是否超过阀值（next），满足则输出
 *  再继续找，找到超过阀值的，则匹配下一个是否超过阀值（next）
 *
 */
public class CEPExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TemperatureEvent> inputEventStream =
                env.fromElements(
                        new TemperatureEvent(1, "xyz", 22.0),
                        new TemperatureEvent(1, "xyz", 20.1),
                        new TemperatureEvent(1, "xyz", 21.1),
                        new TemperatureEvent(1, "xyz", 22.2),
                        new TemperatureEvent(1, "xyz", 22.1),
                        new TemperatureEvent(2, "xyz", 22.3),
                        new TemperatureEvent(1, "xyz", 22.1),
                        new TemperatureEvent(2, "xyz", 22.4),
                        new TemperatureEvent(2, "xyz", 27.0),
                        new TemperatureEvent(2, "xyz", 27.7),
                        new TemperatureEvent(2, "xyz", 28.7),
                        new TemperatureEvent(2, "xyz", 29.0));

        Pattern<TemperatureEvent, ?> warningPattern = Pattern.<TemperatureEvent>begin("first")//
                .subtype(TemperatureEvent.class)
                .where(new MyFirstSimpleCondition())
                .next("second")
                .subtype(TemperatureEvent.class)
                .where(new MySecondSimpleCondition())
                .within(Time.seconds(10)); // 在10s内 如果两个事件的温度都超过阀值。则产生一个温度报警

        PatternStream<TemperatureEvent> tempPatternStream = CEP.pattern(inputEventStream.keyBy("rockID"), warningPattern);


        DataStream<TemperatureWarning> warning = tempPatternStream.select(new PatternSelectFunction<TemperatureEvent, TemperatureWarning>() {
            @Override
            public TemperatureWarning select(Map<String, List<TemperatureEvent>> map) throws Exception {
                TemperatureEvent first = (TemperatureEvent) map.get("first").get(0);
                TemperatureEvent second = (TemperatureEvent) map.get("second").get(0);
                System.out.println(map);
                return new TemperatureWarning(first.getRockID(), (first.getTemperature() + second.getTemperature()) / 2);
            }
        });


        // 当一个机架在20s内连续两个升温警告,则发出Alert
        Pattern<TemperatureWarning, ?> temperatureWarningPattern = Pattern.<TemperatureWarning>begin("start")
                .next("second").within(Time.seconds(20));

        DataStream<TemperatureAlert> result = CEP.pattern(warning.keyBy("rockID"), temperatureWarningPattern).flatSelect(new PatternFlatSelectFunction<TemperatureWarning, TemperatureAlert>() {
            @Override
            public void flatSelect(Map<String, List<TemperatureWarning>> map, Collector<TemperatureAlert> collector) throws Exception {

                List<TemperatureWarning> firstList = map.get("start");
                TemperatureWarning first = new TemperatureWarning();
                if (firstList != null && firstList.size() != 0) {
                    first = firstList.get(0);
                }

                List<TemperatureWarning> secondList = map.get("second");
                TemperatureWarning second = new TemperatureWarning();
                if (secondList != null && secondList.size() != 0) {
                    second = secondList.get(0);
                }
                System.out.println(map);
                if (first.getTemperature() < second.getTemperature()) {
                    collector.collect(new TemperatureAlert(first.getRockID()));
                }

            }
        });


        result.print();

        env.execute("cep example");

    }

    // 简单条件
    private static class MyFirstSimpleCondition extends SimpleCondition<TemperatureEvent> {
        @Override
        public boolean filter(TemperatureEvent temperatureEvent) throws Exception {
            System.out.println("first:"+temperatureEvent);
            return temperatureEvent.getTemperature() >= 26;
        }
    }

    // 简单条件
    private static class MySecondSimpleCondition extends SimpleCondition<TemperatureEvent> {
        @Override
        public boolean filter(TemperatureEvent temperatureEvent) throws Exception {
            System.out.println("second:"+temperatureEvent);
            return temperatureEvent.getTemperature() >= 26;
        }
    }
}
