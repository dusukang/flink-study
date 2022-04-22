package com.flink.flinksql;

import com.flink.flinkcommon.checkpoint.FsCheckPoint;
import com.flink.flinkcommon.constant.SystemConstant;
import com.flink.flinkcommon.enums.JobTypeEnum;
import com.flink.flinkcommon.execute.ExecuteSql;
import com.flink.flinkcommon.model.SqlCommandCall;
import com.flink.flinkcommon.model.SqlJobRunParam;
import com.flink.flinkcommon.sql.SqlFileParser;
import com.flink.flinkcommon.util.ParamUtil;
import org.apache.flink.api.common.JobID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class SqlJobApplication {
    private static final Logger log = LoggerFactory.getLogger(SqlJobApplication.class);
    public static void main(String[] args) throws Exception {
        Arrays.stream(args).forEach(arg -> log.warn(arg));
        SqlJobRunParam jobRunParam = ParamUtil.buildSqlJobParam(args);
        List<String> sql = Files.readAllLines(Paths.get(jobRunParam.getSqlPath()));
        List<SqlCommandCall> sqlCommandCallList = SqlFileParser.fileToSql(sql);
        EnvironmentSettings settings = null;
        TableEnvironment tEnv = null;
        StreamExecutionEnvironment env = null;
        if (jobRunParam.getJobTypeEnum() != null && JobTypeEnum.SQL_BATCH.equals(jobRunParam.getJobTypeEnum())) {
            log.info("[SQL_BATCH]本次任务是批任务");
            //批处理
            settings = EnvironmentSettings.newInstance()
                    .useBlinkPlanner()
                    .inBatchMode()
                    .build();
            tEnv = TableEnvironment.create(settings);
        } else {
            log.info("[SQL_STREAMING]本次任务是流任务");
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            //默认是流 流处理 目的是兼容之前版本
            settings = EnvironmentSettings.newInstance()
                    .useBlinkPlanner()
                    .inStreamingMode()
                    .build();
            tEnv = StreamTableEnvironment.create(env, settings);
            //设置checkPoint
            FsCheckPoint.setCheckpoint(env, jobRunParam.getCheckPointParam());
        }
        StatementSet statementSet = tEnv.createStatementSet();
        ExecuteSql.exeSql(sqlCommandCallList, tEnv, statementSet);
        TableResult tableResult = statementSet.execute();
        if (tableResult == null || tableResult.getJobClient().get() == null
                || tableResult.getJobClient().get().getJobID() == null) {
            throw new RuntimeException("任务运行失败 没有获取到JobID");
        }
        JobID jobID = tableResult.getJobClient().get().getJobID();
        log.info(SystemConstant.QUERY_JOBID_KEY_WORD + "{}", jobID);
    }
}
