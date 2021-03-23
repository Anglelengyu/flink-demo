package cn.ztash.demo.flink4watermarks;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zhongtao
 * @version V1.0.0
 * @description
 * @date 2021/3/19
 */
public class WaterMarksDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);  //设置时间分配器

        env.setParallelism(1);  //设置并行度
        env.getConfig().setAutoWatermarkInterval(3000);//每9秒发出一个watermark

        DataStream<String> text = env.socketTextStream("localhost", 9900);

        DataStream<Tuple3<String, Long, Integer>> counts = text.filter(new FilterClass()).map(new LineSplitter())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>>() {

                    private long currentMaxTimestamp = 0l;

                    private final long maxOutOfOrderness = 3000l;   //这个控制失序已经延迟的度量

                    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    //获取EventTime
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element, long previousElementTimestamp) {
                        long timestamp = element.f1;
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        System.out.println(
                                "get timestamp is " + timestamp + " currentMaxTimestamp " + currentMaxTimestamp + sdf.format(new Date(currentMaxTimestamp)));
                        return timestamp;
                    }

                    //获取Watermark
                    @Override
                    public Watermark getCurrentWatermark() {
                        System.out.println("wall clock is " + System.currentTimeMillis() + " new watermark "
                                + (currentMaxTimestamp - maxOutOfOrderness));
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }
                }).keyBy(0).timeWindow(Time.seconds(5))
                // .allowedLateness(Time.seconds(10))
                .sum(2);

        counts.print();
        env.execute("Window WordCount");

    }

//    自定义获取timesStamp
//    private static class MyTimestamp extends AscendingTimestampExtractor<Tuple3<String, Long, Integer>> {
//
//        private static final long serialVersionUID = 1L;
//
//        public long extractAscendingTimestamp(Tuple3<String, Long, Integer> element) {
//
//            return element.f1;
//        }
//
//    }

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

