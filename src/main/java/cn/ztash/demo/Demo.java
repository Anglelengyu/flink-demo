package cn.ztash.demo;


import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.util.DateTimeStringUtils;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author zhongtao
 * @version V1.0.0
 * @description
 * @date 2021/3/22
 */
public class Demo {
    public static void main(String[] args) {
//        System.out.println(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(sdf.format(new Date(1616382150000l)));

        Long x = -1000_000_000L;
        Long y = 1000_000_000L;
        long r = x / y;
        // if the signs are different and modulo not zero, round down
        if ((x ^ y) < 0 && (r * y != x)) {
            r--;
        }
        System.out.println(r);

    }
}
