package com.flink.flinkstream.bean;

import lombok.Data;

import java.io.Serializable;

@Data
public class Stu implements Serializable {
    private Integer stuId;
    private Integer proId;
    private Double score;
    private Long timestamp;
}
