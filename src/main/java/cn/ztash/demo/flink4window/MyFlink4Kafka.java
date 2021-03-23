package cn.ztash.demo.flink4window;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Date;
import java.util.Properties;

public class MyFlink4Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka 连接配置
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.11.1.20:9092");
        properties.put("zookeeper.connect", "10.11.1.20:2181");
        properties.put("group.id", "zt-flink-group");
        properties.put("auto.offset.reset", "latest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 添加数据源
        SingleOutputStreamOperator<MyInfo> source = env.addSource(
                // 从kafka消费
                new FlinkKafkaConsumer<String>(
                        "zt_test",
                        new SimpleStringSchema(),
                        properties))
                .setParallelism(1)
                // map操作，转换，从一个数据流转换成另一个数据流，这里是从string-->UrlInfo
                .map(msg -> {
                    MyInfo myInfo = JSON.parseObject(msg, MyInfo.class);
                    return myInfo;
                });

        KeyedStream<MyInfo, String> keyedStream = source.keyBy(new KeySelector<MyInfo, String>() {
            @Override
            public String getKey(MyInfo myInfo) throws Exception {
                return myInfo.getKey();
            }
        });

        // 设置时间类型为Processing Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        SingleOutputStreamOperator<MyInfo> windowStream = keyedStream.timeWindow(Time.seconds(10))
                .reduce((ReduceFunction<MyInfo>) (t1, t2) -> {
                    MyInfo myInfo = new MyInfo();

                    myInfo.setKey(t1.getKey());
                    myInfo.setUrl(myInfo.getKey() + "/reduce/" + DateFormatUtils.format(new Date(), "yyyy-MM-dd'T'HH:mm:ss"));
                    myInfo.setHash(null);

                    myInfo.setCount(t1.getCount() + 1);// 在reduce中做累加计数
                    return myInfo;
                }).returns(MyInfo.class);
        // 数据下沉 落库等操作
        windowStream.addSink(new PrintSinkFunction<>());
        env.execute("Flink add kafka data source");
    }
}
