package com.flink.flinkstream.bean;

import lombok.Data;

import java.io.Serializable;

@Data
public class Pro implements Serializable {
    private Integer proId;
    private String proName;
    private Long timestamp;
}
