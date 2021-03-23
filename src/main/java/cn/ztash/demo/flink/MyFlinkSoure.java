package cn.ztash.demo.flink;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MyFlinkSoure extends RichSourceFunction<MyFlinkInfo> {

    private final Logger log = LoggerFactory.getLogger(MyFlinkSoure.class);

    @Override
    public void run(SourceContext<MyFlinkInfo> sourceContext) throws Exception {
        log.info("------run ");
        // 模拟数据
        List<MyFlinkInfo> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            MyFlinkInfo myFlinkInfo = new MyFlinkInfo();
            myFlinkInfo.setName("编号：" + i);
            myFlinkInfo.setAge(i);
            list.add(myFlinkInfo);
        }
        list.parallelStream().forEach(info -> sourceContext.collect(info));
    }

    @Override
    public void cancel() {
        log.info("------cancel ");
    }
}
