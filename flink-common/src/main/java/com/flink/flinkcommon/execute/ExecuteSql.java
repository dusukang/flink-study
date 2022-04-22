package com.flink.flinkcommon.execute;

import com.flink.flinkcommon.config.Configurations;
import com.flink.flinkcommon.log.LogPrint;
import com.flink.flinkcommon.model.SqlCommandCall;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;

import java.util.List;

public class ExecuteSql {

    /**
     * 批量执行执行sql
     */
    public static void exeSql(List<SqlCommandCall> sqlCommandCallList, TableEnvironment tEnv, StatementSet statementSet) {
        for (SqlCommandCall sqlCommandCall : sqlCommandCallList) {
            switch (sqlCommandCall.sqlCommand) {
                //配置
                case SET:
                    Configurations.setSingleConfiguration(tEnv, sqlCommandCall.operands[0],
                            sqlCommandCall.operands[1]);
                    break;
                //insert 语句
                case INSERT_INTO:
                case INSERT_OVERWRITE:
                    LogPrint.logPrint(sqlCommandCall);
                    statementSet.addInsertSql(sqlCommandCall.operands[0]);
                    break;
                //显示语句
                case SELECT:
                case SHOW_CATALOGS:
                case SHOW_DATABASES:
                case SHOW_MODULES:
                case SHOW_TABLES:
                    tEnv.sqlQuery(sqlCommandCall.operands[0]);
                    break;
                default:
                    LogPrint.logPrint(sqlCommandCall);
                    tEnv.executeSql(sqlCommandCall.operands[0]);
                    break;
            }
        }
    }
    
}
