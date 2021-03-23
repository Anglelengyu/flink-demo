package cn.ztash.demo.flink4kafka;

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
        MyFlinkInfo info = new MyFlinkInfo();
        long currentMills = System.currentTimeMillis();
        if (currentMills % 100 > 30) {
            info.setMsg("http://so.com/" + currentMills);
        } else {
            info.setMsg("http://baidu.com/" + currentMills);
        }

        String msgContent = JSON.toJSONString(info); // 确保发送的消息都是string类型
        ProducerRecord record = new ProducerRecord<String, String>(KAFKA_TOPIC, null, msgContent);
        producer.send(record);

        log.debug("send msg:" + msgContent);


        producer.flush();

    }
}
