package cn.ztash.demo.flink4function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

@Slf4j
public class MyFlinkKafkaProduce {

    private static final String KAFKA_TOPIC = "zt_test1";
    private static final String KAFKA_ADDR = "10.11.1.20:9092";
    private static Properties properties;

    private static void init() {
        properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_ADDR);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public static void main(String[] args) throws Exception {
        init();
        send2Kafka();
    }

    private static void send2Kafka() throws Exception {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        File file = new File("E:/ideawork/flink-demo/src/main/resources/demo.json");
        InputStreamReader reader = new InputStreamReader(new FileInputStream(file), "utf-8");
        StringBuffer sb = new StringBuffer();
        int ch = 0;
        while ((ch = reader.read()) != -1) {
            sb.append((char) ch);
        }
        reader.close();
        String json = sb.toString();

        List<MyFlinkInfo> myFlinkInfos = JSONArray.parseArray(json, MyFlinkInfo.class);
        myFlinkInfos.forEach(System.out::println);

        for (MyFlinkInfo myFlinkInfo : myFlinkInfos) {

            String msgContent = JSON.toJSONString(myFlinkInfo); // 确保发送的消息都是string类型
            ProducerRecord record = new ProducerRecord<String, String>(KAFKA_TOPIC, null, msgContent);
            producer.send(record);

            log.debug("send msg:" + msgContent);

            producer.flush();
        }


    }
}
