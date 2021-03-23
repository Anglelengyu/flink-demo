package cn.ztash.demo.flink4kafka;

import lombok.Data;

import java.io.Serializable;

@Data
public class MyFlinkInfo implements Serializable {
    private String msg;
}
