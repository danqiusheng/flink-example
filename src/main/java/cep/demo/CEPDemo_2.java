package cep.demo;

import cep.demo.model.Zone;
import cep.demo.model.ZoneAlert;
import cep.demo.model.ZoneWarn;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import streaming.model.StaEntity;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * 过程： 模拟区域小区信息， * 创建预警并将数据写入到内存数据库(区域异常demo）
 * <p>
 * Created by Administrator on 2018/5/15.
 * 过程： 模拟小区流信息，
 * 携带的流信息有:当前小区id,当前小区的进出类型, 当前小区那时候进或者出的人数,时间戳（正常数据应当指定水印，防止数据延迟到达)
 * 思路：判断当前小区是否为正常数据，先根据流将数据按键分流（小区id），
 *
 * @author danqiusheng
 */
public class CEPDemo_2 {
    public static void main(String[] args) throws Exception {
        // 获取执行环境， 本地执行 采用local环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 在5分钟内，一个区域出的人数和入的人数不平衡，则进行
        // 将这个做为毫秒数
        DataStreamSource<Long> dataStream = env.generateSequence(1, 50);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Zone> result = dataStream.map(new MapFunction<Long, Zone>() {
            @Override
            public Zone map(Long value) throws Exception {
                int zone = value.intValue() % 3;
                int type = value.intValue() % 2;

                // 随机生成数
                Random random = new Random();
                int count = random.nextInt(100);
                // 模拟正常人流 并进行分配捕捉时间以及区域数据
                // 模拟生成数据有id，时间戳,小区id,是出还是入,人数统计等属性
                Zone Zone = new Zone(value + "", value, zone + "", type + "", count);
                System.out.println("输入数据:" + Zone);
                return Zone;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Zone>() {// 分配时间戳  用于延迟事件
            @Override
            public long extractAscendingTimestamp(Zone element) {
                return element.getCatchtime() - 10;
            }
        })
                .keyBy("zone");//.keyBy("type");


        // 匹配CEP 定义规则
        // 注意：编写程序需要再Pattern.<T>begin 泛型T是必须要配置的。
        Pattern<Zone, ?> peopleAlertPattern = Pattern.<Zone>begin("first")
                // .where(new FirstSimpleCondition())//
                // .followedBy("second")             //
                .where(new FirstSimpleCondition())   //
                .timesOrMore(5)                      // 时间次数
                .within(Time.milliseconds(3000));    // 设置时间窗口 tumbling翻滚类型


        // 获取匹配结果
//        DataStream<Zone> stream = CEP.pattern(result, peopleAlertPattern).select(new PatternSelectFunction<Zone, Zone>() {
//            @Override
//            public Zone select(Map<String, List<Zone>> map) throws Exception {
//                System.out.println(map);
//                // 在时间窗口内根据CEP规则获取匹配的数据，判断小区是否为正常数据，如果出现不正常数据，则判断当前小区为异常数据.
//                // 在里面获取CEP规则获取的数据，然后对其进行处理
//                // 模拟情况，如果出和入相差100，则判断当前小区为异常小区，发出警告数据
//
//                return map.get("first").get(0);// 这一步,会将挑出的数据传递到流中，用steam.print()可以看到效果
//            }
//        });

        // 获取异常的数据并进行预警
        DataStream<ZoneAlert> stream = CEP.pattern(result, peopleAlertPattern).flatSelect(new PatternFlatSelectFunction<Zone, ZoneAlert>() {
            @Override
            public void flatSelect(Map<String, List<Zone>> map, Collector<ZoneAlert> collector) throws Exception {
                // 在时间窗口内根据CEP规则获取匹配的数据，判断小区是否为正常数据，如果出现不正常数据，则判断当前小区为异常数据.
                // 在里面获取CEP规则获取的数据，然后对其进行处理
                // 模拟情况，如果出和入相差100，则判断当前小区为异常小区，发出警告数据
                // 将map里面的数据进行匹配 比较，然后判断是否为异常小区（出和入的数据比较）
                List<Zone> list = map.get("first");// 获取五秒窗口内的小区进出数据
                if (list == null) return;
                long in = 0;
                long out = 0;
                String zoneId = "null";
                String inId = "";
                String outId = "";
                for (Zone zone : list) {
                    if (zone.getType().equals("1")) {
                        //
                        in += zone.getCount();
                        zoneId = zone.getZone();
                        inId += zone.getId()+",";
                    } else if (zone.getType().equals("0")) {
                        //
                        out += zone.getCount();
                        outId += zone.getId()+",";
                    }
                }

                if (Math.abs(in - out) > 30) {
                    // 发出警告
                    System.out.println(map);
                    collector.collect(new ZoneAlert(zoneId, "This zone has some exception , inId:"+inId+",outId: "+outId+"", new Long(Math.abs(in - out)).intValue()));
                }
            }
        });

        stream.print();

        // 可以继续定义规则，比如在5秒内出现了2次，则进行报警
        // 因为涉及到不同小区数据，所以需要分开
        DataStream<ZoneAlert> twoStream = stream.keyBy("zone");
        Pattern<ZoneAlert, ?> zoneWarnPattern = Pattern
                .<ZoneAlert>begin("start")
                .next("second")
                .within(Time.seconds(5));

        DataStream<ZoneWarn> finalStream = CEP.pattern(twoStream, zoneWarnPattern).flatSelect(new PatternFlatSelectFunction<ZoneAlert, ZoneWarn>() {
            @Override
            public void flatSelect(Map<String, List<ZoneAlert>> map, Collector<ZoneWarn> out) throws Exception {
                System.out.println("final:" + map);
            }
        });

        // 模拟数据将检测到匹配的事件打印到控制台
        // 可以自定sink到数据库
        finalStream.print();
        // result.print();
        env.execute("test cep demo 2");
    }

    // 第一个简单条件判断
    static class FirstSimpleCondition extends SimpleCondition<Zone> {
        @Override
        public boolean filter(Zone value) throws Exception {
            // 特定判断当前的次数是否超出
            if (value.getCount() > 50) {
                return true;
            }
            return false;
        }
    }
}
