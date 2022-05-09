package com.flink.flinksql.bean;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class UserLoginTimes extends CdcDeserializeBaseModel{

   private Long uid;

   private BigDecimal window_start;

   private BigDecimal window_end;

   private Long login_times;

}
