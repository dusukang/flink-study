package com.flink.flinksql.bean;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class StudentProject extends CdcDeserializeBaseModel{

    private Integer stu_id;

    private String stu_name;

    private Long pro_id;

    private String pro_name;

    private BigDecimal score;

    private BigDecimal total_score;

}
