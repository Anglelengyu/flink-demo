package cn.ztash.demo.group;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhongtao
 * @version V1.0.0
 * @description
 * @date 2021/3/23
 */
// ((()())()) 拆解
public class GroupDemo {

    public static void main(String[] args) {
        String str = "(a(b(c)d(e)f)g(h)i)";
        char[] chars = str.toCharArray();

        int start = 0;
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < chars.length; i++) {

            if (chars[i] == '(') {
                start++;
                map.put(start, i);
            } else if (chars[i] == ')') {
                System.out.println(str.substring(map.get(start) + 1, i));
                start--;
            }
        }

    }
}
