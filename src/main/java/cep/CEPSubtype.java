package cep;

import cep.pojo.Alert;
import cep.pojo.TemperatureEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * target:设置子类型
 * output:输出温度大于26度以上的数据
 */
public class CEPSubtype {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TemperatureEvent> inputEventStream =
                env.fromElements(new TemperatureEvent("xyz", 22.0),
                        new TemperatureEvent("xyz", 20.1),
                        new TemperatureEvent("xyz", 21.1),
                        new TemperatureEvent("xyz", 22.2),
                        new TemperatureEvent("xyz", 22.1),
                        new TemperatureEvent("xyz", 22.3),
                        new TemperatureEvent("xyz", 22.1),
                        new TemperatureEvent("xyz", 22.4),
                        new TemperatureEvent("xyz", 27.7),
                        new TemperatureEvent("xyz", 27.0));
        Pattern<TemperatureEvent, ?> warningPattern = Pattern.<TemperatureEvent>begin("first")//
                .subtype(TemperatureEvent.class)//
                .where(new FilterThanFunction());//
        PatternStream<TemperatureEvent> patternStream = CEP.pattern(inputEventStream, warningPattern);
        patternStream.select(new MyPatternSelectFunction()).print();
        env.execute("CEP subtype");

    }

    private static class MyPatternSelectFunction implements PatternSelectFunction<TemperatureEvent, Alert> {
        public Alert select(Map<String, List<TemperatureEvent>> map) throws Exception {
            TemperatureEvent startEvent = map.get("first").get(0);
            return new Alert("temperature:" + startEvent.getTemperature() + ";machine name:" + startEvent.getMachineName());
        }
    }

    private static class FilterThanFunction extends IterativeCondition<TemperatureEvent> {
        @Override
        public boolean filter(TemperatureEvent temperatureEvent, Context<TemperatureEvent> context) throws Exception {
            if (temperatureEvent.getTemperature() >= 26.0) {
                return true;
            }
            return false;
        }
    }
}
