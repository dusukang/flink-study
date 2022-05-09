package com.flink.flinksql.bean;

import lombok.Data;

@Data
public class User extends CdcDeserializeBaseModel{

    private Long uid;

    private String uname;

    private String others;

}
