package com.flink.flinkstream.bean;

import lombok.Data;

@Data
public class UserStatic{
    private String uid;
    private Long visitCount;
    private Long joinCount;
    private Long buyCount;
}
