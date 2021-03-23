package cn.ztash.demo.test;

import lombok.Data;

/**
 * @author zhongtao
 * @version V1.0.0
 * @description
 * @date 2021/3/22
 */
public class Bean2Map {

    public static void main(String[] args) {
        BeanDemo beanDemo = new BeanDemo();
        beanDemo.setName("zhang san");
        beanDemo.setAge(18);
        beanDemo.setInterest("lol");
        
    }

    @Data
    public static class BeanDemo {

        private String name;
        private Integer age;
        private String interest;
    }
}
