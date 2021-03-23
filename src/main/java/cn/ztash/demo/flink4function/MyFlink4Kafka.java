package cn.ztash.demo.flink4function;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

public class MyFlink4Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //十分钟保存一个checkpoint
        env.enableCheckpointing(600000L);
        env.getCheckpointConfig().setCheckpointTimeout(20000L);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        // kafka 连接配置
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.11.1.20:9092");
        properties.put("zookeeper.connect", "10.11.1.20:2181");
        properties.put("group.id", "zt-flink-group1");
        properties.put("auto.offset.reset", "latest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 添加数据源
        DataStream<MyFlinkInfo> source = env.addSource(
                // 从kafka消费
                new FlinkKafkaConsumer<String>(
                        "zt_test1",
                        new SimpleStringSchema(),
                        properties))
                .process(new MyKafkaProcessFunction());

        // 注册
        tableEnvironment.createTemporaryView("kafka_data", source);

        // 构建sql
        String sql = ""
                + "	select "
//                + "		TUMBLE_START(time, INTERVAL '1' SECOND) as window_start, "
//                + "		TUMBLE_END(time, INTERVAL '1' SECOND) as window_end, "
                + "		count(1)"
//                + "		userID,"
//                + "		count(userID) as browsePV"
//                + "		sum(productPrice) as s"
                + "	from kafka_data ";
//                + "	group by userID";
//        "SELECT user, TUMBLE_START(rowtime, INTERVAL ‘1’ DAY) as wStart, SUM(amount) FROM Orders GROUP BY TUMBLE(rowtime, INTERVAL ‘1’ DAY), user;\n"

        // 执行sql
        Table table = tableEnvironment.sqlQuery(sql);
        tableEnvironment.toRetractStream(table, Row.class).print();

        System.out.println(tableEnvironment.explainSql(sql));

        // 数据下沉 落库等操作
//        returns.addSink(new PrintSinkFunction<>());
        env.execute("Flink add kafka data source");
    }
}
