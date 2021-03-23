package cn.ztash.demo.flink4function;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
public class MyFlinkInfo implements Serializable {
    private String userID;
    private Date eventTime;
    private Long time;
    private String eventType;
    private String productID;
    private Integer productPrice;

}
