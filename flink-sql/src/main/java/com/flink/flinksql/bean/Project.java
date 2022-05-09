package com.flink.flinksql.bean;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class Project extends CdcDeserializeBaseModel{

    private Long pro_id;

    private String pro_name;

    private BigDecimal total_score;

}
