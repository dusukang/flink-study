package com.flink.flinkcommon.util;

import com.flink.flinkcommon.checkpoint.CheckPointParams;
import com.flink.flinkcommon.enums.JobTypeEnum;
import com.flink.flinkcommon.model.SqlJobRunParam;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

public class ParamUtil {

    /**
     * sql作业
     * @param args
     * @return
     */
    public static SqlJobRunParam buildSqlJobParam(String[] args){
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String sqlPath = parameterTool.get("sql");
        Preconditions.checkNotNull(sqlPath, "-sql参数不能为空");
        SqlJobRunParam jobRunParam = new SqlJobRunParam();
        jobRunParam.setSqlPath(sqlPath);
        jobRunParam.setCheckPointParam(CheckPointParams.buildCheckPointParam(parameterTool));
        String type = parameterTool.get("type");
        if (StringUtils.isNotEmpty(type)) {
            jobRunParam.setJobTypeEnum(JobTypeEnum.getJobTypeEnum(Integer.valueOf(type)));
        }
        return jobRunParam;
    }

}
