package cn.ztash.demo.flink4cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author zhongtao
 * @version V1.0.0
 * @description
 * @date 2021/3/19
 */
public class FlinkCEPDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);  //设置时间分配器

        env.setParallelism(1);  //设置并行度
//        env.getConfig().setAutoWatermarkInterval(3000);//每9秒发出一个watermark

        DataStream<String> text = env.socketTextStream("localhost", 9900);

        DataStream<Tuple3<String, Long, Integer>> counts = text.filter(new FilterClass()).map(new LineSplitter())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Integer>>
                        forBoundedOutOfOrderness(Duration.ofSeconds(10,3))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, Long, Integer>>) (tuple, l) -> tuple.f1));
//        counts.print();

        Pattern<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>> pattern =
                Pattern.<Tuple3<String, Long, Integer>>begin("fi" +
                        "rst", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(new IterativeCondition<Tuple3<String, Long, Integer>>() {

                            @Override
                            public boolean filter(Tuple3<String, Long, Integer> tuple3, Context<Tuple3<String, Long, Integer>> context) throws Exception {
                                System.out.println("first: " + tuple3);
                                return tuple3.f0.equals("aa");
                            }
                        }).next("next").where(
                        new SimpleCondition<Tuple3<String, Long, Integer>>() {
                            @Override
                            public boolean filter(Tuple3<String, Long, Integer> tuple3) {
                                System.out.println("next: " + tuple3);
                                return tuple3.f0.equals("aa");
                            }
                        }).within(Time.seconds(3));
//        Pattern<Tuple3<String, String, String>, Tuple3<String, String, String>> myPattern =
//                Pattern.<Tuple3<String, String, String>>begin("start").where(new IterativeCondition<Tuple3<String, String, String>>() {
//                    @Override
//                    public boolean filter(Tuple3<String, String, String> value, Context<Tuple3<String, String, String>> ctx) throws Exception {
//                        System.out.println("value:" + value);
//                        return value.f2.equals("order");
//                    }
//                })

        PatternStream<Tuple3<String, Long, Integer>> patternStream = CEP.pattern(counts, pattern);

//        patternStream.select(
//                new PatternSelectFunction<Tuple3<String, Long, Integer>, String>() {
//                    @Override
//                    public String select(Map<String, List<Tuple3<String, Long, Integer>>> map) throws Exception {
//                        System.out.println("first : ***" + map.get("first"));
//                        Tuple3<String, Long, Integer> next = map.get("next").get(0);
//                        System.out.println("next" + next);
//                        return next.f0 + "---->" + next.f1 + "----->" + next.f2;
//                    }
//                }
//        ).print();

        patternStream.process(new PatternProcessFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>>() {
            @Override
            public void processMatch(Map<String, List<Tuple3<String, Long, Integer>>> map, Context context, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                System.out.println("******************************");
                System.out.println(map);
                collector.collect(map.entrySet().iterator().next().getValue().get(0));
            }
        }).print();
        env.execute("Window WordCount");

    }


    //构造出element以及它的event time.然后把次数赋值为1
    public static final class LineSplitter implements MapFunction<String, Tuple3<String, Long, Integer>> {

        @Override
        public Tuple3<String, Long, Integer> map(String value) throws Exception {
            // TODO Auto-generated method stub
            String[] tokens = value.toLowerCase().split("\\W+");

            long eventtime = Long.parseLong(tokens[1]);

            return new Tuple3<String, Long, Integer>(tokens[0], eventtime, 1);
        }
    }

    //过滤掉为null和whitespace的字符串
    public static final class FilterClass implements FilterFunction<String> {

        @Override
        public boolean filter(String value) throws Exception {

            if (StringUtils.isNullOrWhitespaceOnly(value)) {
                return false;
            } else {
                return true;
            }
        }

    }
}

