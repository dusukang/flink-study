package com.flink.flinksql.bean;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class Order extends CdcDeserializeBaseModel{

    private Long order_id;

    private Integer order_type;

    private BigDecimal cost;

    private String order_name;

}
