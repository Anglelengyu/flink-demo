package cn.ztash.demo.flink4function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class MyKafkaProcessFunction extends ProcessFunction<String, MyFlinkInfo> {
    @Override
    public void processElement(String s, Context context, Collector<MyFlinkInfo> collector) throws Exception {
//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
        MyFlinkInfo myFlinkInfo = JSONObject.parseObject(s, MyFlinkInfo.class);
        myFlinkInfo.setUserID(myFlinkInfo.getUserID() + "***********");
        myFlinkInfo.setTime(myFlinkInfo.getEventTime().getTime());
        collector.collect(myFlinkInfo);
    }
}
