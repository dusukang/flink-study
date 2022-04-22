package com.flink.flinkcommon.model;

import com.flink.flinkcommon.enums.JobTypeEnum;
import lombok.Data;

@Data
public class SqlJobRunParam {
    /**
     * sql语句目录
     */
    private String sqlPath;

    /**
     * 任务类型
     */
    private JobTypeEnum jobTypeEnum;

    /**
     * CheckPoint 参数
     */
    private CheckPointParam checkPointParam;



}
