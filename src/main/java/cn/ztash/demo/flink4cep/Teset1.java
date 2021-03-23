package cn.ztash.demo.flink4cep;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author zhongtao
 * @version V1.0.0
 * @description
 * @date 2021/3/23
 */
public class Teset1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, String, String>> loginEventStream = env.fromCollection(Arrays.asList(
                Tuple3.of("小明", "192.168.0.1", "fail"),
                Tuple3.of("小明", "192.168.0.2", "fail"),
                Tuple3.of("小王", "192.168.10,11", "fail"),
                Tuple3.of("小王", "192.168.10,12", "fail"),
                Tuple3.of("小明", "192.168.0.3", "fail"),
                Tuple3.of("小明", "192.168.0.4", "fail"),
                Tuple3.of("小王", "192.168.10,10", "success")
        ));

        DataStream<String> text = env.socketTextStream("localhost", 9900);

        DataStream<Tuple3<String, Long, Integer>> counts = text.filter(new FlinkCEPDemo1.FilterClass()).map(new FlinkCEPDemo1.LineSplitter());

        // 在3秒 内重复登录了三次, 则产生告警
        Pattern<Tuple3<String, String, String>, Tuple3<String, String, String>> loginFailPattern = Pattern.<Tuple3<String, String, String>>
                begin("first")
                .where(new IterativeCondition<Tuple3<String, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, String> loginEvent, Context<Tuple3<String, String, String>> context) throws Exception {
                        System.out.println("first: " + loginEvent);
                        return loginEvent.f2.equals("fail");
                    }
                })
                .next("second")
                .where(new IterativeCondition<Tuple3<String, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, String> loginEvent, Context<Tuple3<String, String, String>> context) throws Exception {
                        System.out.println("second: " + loginEvent);
                        return loginEvent.f2.equals("fail");
                    }
                })
                .next("three")
                .where(new IterativeCondition<Tuple3<String, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, String> loginEvent, Context<Tuple3<String, String, String>> context) throws Exception {
                        System.out.println("three: " + loginEvent);
                        return loginEvent.f2.equals("fail");
                    }
                })
                .within(Time.seconds(3));

        // 根据用户id分组，以便可以锁定用户IP，cep模式匹配
        PatternStream<Tuple3<String, String, String>> patternStream = CEP.pattern(
                loginEventStream.keyBy((tuple) ->tuple.f0),
                loginFailPattern);

        // 获取重复登录三次失败的用户信息
        DataStream<String> loginFailDataStream = patternStream.select(
                new PatternSelectFunction<Tuple3<String, String, String>, String>() {
                    @Override
                    public String select(Map<String, List<Tuple3<String, String, String>>> pattern) throws Exception {
                        List<Tuple3<String, String, String>> second = pattern.get("three");
                        return second.get(0).f0 + ", " + second.get(0).f1 + ", " + second.get(0).f2;
                    }
                }
        );

        // 打印告警用户
        loginFailDataStream.print();
        env.execute();
    }

    public static class LoginEvent {
        private String userId;
        private String ip;
        private String type;

        public LoginEvent(String userId, String ip, String type) {
            this.userId = userId;
            this.ip = ip;
            this.type = type;
        }

        public String getUserId() {
            return userId;
        }

        public String getType() {
            return type;
        }

        public String getIp() {
            return ip;
        }

        @Override
        public String toString() {
            return "LoginEvent{" +
                    "userId='" + userId + '\'' +
                    ", type='" + type + '\'' +
                    ", ip='" + ip + '\'' +
                    '}';
        }

    }
}
