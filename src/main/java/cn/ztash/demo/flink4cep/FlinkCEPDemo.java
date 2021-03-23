package cn.ztash.demo.flink4cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.codehaus.groovy.jsr223.GroovyScriptEngineFactory;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author zhongtao
 * @version V1.0.0
 * @description
 * @date 2021/3/19
 */
public class FlinkCEPDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);  //设置时间分配器

        env.setParallelism(1);  //设置并行度
        env.getConfig().setAutoWatermarkInterval(3000);//每9秒发出一个watermark

        DataStream<String> text = env.socketTextStream("localhost", 9900);

        DataStream<Tuple3<String, Long, Integer>> counts = text.filter(new FilterClass()).map(new LineSplitter())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Integer>>
                        forBoundedOutOfOrderness(Duration.ofSeconds(5, 3))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, Long, Integer>>) (tuple, l) -> tuple.f2));

        String script = "\n" +
                "        import functions.cep.definedSimpleFunction.ProcessSimpleConditionFunction\n" +
                "        import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy\n" +
                "        import org.apache.flink.cep.pattern.Pattern\n" +
                "        import org.apache.flink.streaming.api.windowing.time.Time\n" +
                "        def getPattern() {" +
                "        return Pattern.begin<Tuple3<String, Long, Integer>>(\"开始执行\").where(tuple.f0.equals(\"aa\"));" +
//                "            return Pattern.begin(\"执行cmd\", AfterMatchSkipStrategy.skipPastLastEvent()).subtype(ProcessData.class)" +
//                "                    .where(new ProcessSimpleConditionFunction(\"wordsIn(processName,'0','cmd')\")).oneOrMore()" +
//                "                    .followedBy(\"执行ping\").subtype(ProcessData.class)" +
//                "                    .where(new ProcessSimpleConditionFunction(\"wordsIn(processName,'0','ping')\")).oneOrMore()" +
//                "                    .followedBy(\"执行curl命令\").subtype(ProcessData.class)" +
//                "                    .where(new ProcessSimpleConditionFunction(\"wordsIn(processName,'0','curl')\")).times(3).within(Time.seconds(60));" +
                "        }";


        GroovyScriptEngineFactory groovyScriptEngineFactory = new GroovyScriptEngineFactory();
        ScriptEngine engine = groovyScriptEngineFactory.getScriptEngine();
        engine.eval(script);
        Invocable inv = (Invocable) engine;
        Pattern pattern = (Pattern) inv.invokeFunction("getPattern");

        PatternStream<Tuple3<String, Long, Integer>> patternStream = CEP.pattern(counts, pattern);

        patternStream.process(new PatternProcessFunction<Tuple3<String, Long, Integer>, Object>() {
            @Override
            public void processMatch(Map<String, List<Tuple3<String, Long, Integer>>> map, Context context, Collector<Object> collector) throws Exception {
                collector.collect(map.entrySet().iterator().next().getValue());
            }
        }).addSink(new PrintSinkFunction());
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

