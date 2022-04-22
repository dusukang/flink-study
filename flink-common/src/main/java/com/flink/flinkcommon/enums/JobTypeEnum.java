package com.flink.flinkcommon.enums;

import lombok.Getter;


@Getter
public enum JobTypeEnum {

    SQL_STREAMING(0), JAR(1), SQL_BATCH(2);

    private int code;

    JobTypeEnum(int code) {
        this.code = code;
    }

    public static JobTypeEnum getJobTypeEnum(Integer code) {
        if (code == null) {
            return null;
        }
        for (JobTypeEnum jobTypeEnum : JobTypeEnum.values()) {
            if (code == jobTypeEnum.getCode()) {
                return jobTypeEnum;
            }
        }

        return null;
    }

}
