package cn.ztash.demo.flink4kafka;

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
        SingleOutputStreamOperator<String> source = env.addSource(
                // 从kafka消费
                new FlinkKafkaConsumer<String>(
                        "zt_test",
                        new SimpleStringSchema(),
                        properties))
                .setParallelism(1)
                .map(e -> "key :" + e); // 数据转换

        KeyedStream<String, String> keyedStream = source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
//                String key = JSONObject.parseObject(s).get("msg").toString().substring(0,16);

                return s;
            }
        });

        // 设置时间类型为Processing Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        SingleOutputStreamOperator<String> returns = keyedStream.timeWindow(Time.seconds(10))
                .reduce((ReduceFunction<String>) (str1, str2) -> {
                    return str1 + ": " + str2;
                }).returns(String.class);
        // 数据下沉 落库等操作
        returns.addSink(new PrintSinkFunction<>());
        env.execute("Flink add kafka data source");
    }
}
