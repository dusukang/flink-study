package com.flink.flinksql.bean;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderProduct extends CdcDeserializeBaseModel{

    private Long pro_id;

    private String pro_name;

    private Integer pro_type;

    private Long order_id;

    private Integer order_type;

    private String order_name;

    private String area;

    private BigDecimal cost;

}
