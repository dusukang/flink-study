package com.flink.flinkstream.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Event {
    public String user;
    public String url;
    public Long timestamp;
}
