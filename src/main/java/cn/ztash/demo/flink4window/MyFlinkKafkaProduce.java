package cn.ztash.demo.flink4window;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class MyFlinkKafkaProduce {

    private static final String KAFKA_TOPIC = "zt_test";
    private static final String KAFKA_ADDR = "10.11.1.20:9092";
    private static Properties properties;

    private static void init() {
        properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_ADDR);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public static void main(String[] args) throws InterruptedException {
        init();
        while (true) {
            Thread.sleep(2000);
            send2Kafka();
        }
    }

    private static void send2Kafka() {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        MyInfo info = new MyInfo();
        long currentMills = System.currentTimeMillis();
        if (currentMills % 3 == 0) {
            info.setKey("group0");
            info.setUrl("http://baidu.com/0");
        } else if (currentMills % 3 == 1) {
            info.setKey("group1");
            info.setUrl("http://baidu.com/1");
        } else {
            info.setKey("group2");
            info.setUrl("http://baidu.com/2");
        }

        String msgContent = JSON.toJSONString(info); // 确保发送的消息都是string类型
        ProducerRecord record = new ProducerRecord<String, String>(KAFKA_TOPIC, null, msgContent);
        producer.send(record);

        log.debug("send msg:" + msgContent);


        producer.flush();

    }
}
