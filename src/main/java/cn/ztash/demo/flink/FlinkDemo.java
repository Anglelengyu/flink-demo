package cn.ztash.demo.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

public class FlinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<MyFlinkInfo> source = env.addSource(new MyFlinkSoure());
//        source.addSink(new PrintSinkFunction<>());
//        env.execute();
        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stringStream = env.fromElements("a", "b", "c");
// 把两个流连接在一起: 貌合神离
        ConnectedStreams<Integer, String> cs = intStream.connect(stringStream);
        cs.getFirstInput().print("first");
        cs.getSecondInput().print("second");
        env.execute();
    }
}
