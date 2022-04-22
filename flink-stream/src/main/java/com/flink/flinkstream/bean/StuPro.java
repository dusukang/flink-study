package com.flink.flinkstream.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StuPro implements Serializable {
    private Integer stuId;
    private Integer proId;
    private String proName;
    private Double score;
    private Long stuTimestamp;
    private Long proTimestamp;
}
