package com.flink.flinkcommon.log;

import com.flink.flinkcommon.enums.SqlCommand;
import com.flink.flinkcommon.model.SqlCommandCall;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.TableEnvironment;

@Slf4j
public class LogPrint {

    /**
     * 打印SqlCommandCall 日志信息
     *
     */
    public static void logPrint(SqlCommandCall sqlCommandCall) {
        if (sqlCommandCall == null) {
            throw new NullPointerException("sqlCommandCall is null");
        }
        System.out.println("\n #############" + sqlCommandCall.sqlCommand.name() + "############# \n"
                + sqlCommandCall.operands[0]);
        log.info("\n #############{}############# \n {}", sqlCommandCall.sqlCommand.name(),
                sqlCommandCall.operands[0]);
    }

    /**
     * show 语句  select语句结果打印
     */
    public static void queryRestPrint(TableEnvironment tEnv, SqlCommandCall sqlCommandCall) {
        if (sqlCommandCall == null) {
            throw new NullPointerException("sqlCommandCall is null");
        }
        LogPrint.logPrint(sqlCommandCall);


        if (sqlCommandCall.getSqlCommand().name().equalsIgnoreCase(SqlCommand.SELECT.name())) {
            throw new RuntimeException("目前不支持select 语法使用");
        } else {
            tEnv.executeSql(sqlCommandCall.operands[0]).print();
        }

//        if (sqlCommandCall.getSqlCommand().name().equalsIgnoreCase(SqlCommand.SELECT.name())) {
//            Iterator<Row> it = tEnv.executeSql(sqlCommandCall.operands[0]).collect();
//            while (it.hasNext()) {
//                String res = String.join(",", PrintUtils.rowToString(it.next()));
//                log.info("数据结果 {}", res);
//            }
//        }
    }

}
