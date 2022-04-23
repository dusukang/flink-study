package com.flink.flinkcommon.util;


import com.flink.flinkcommon.enums.SqlCommand;
import com.flink.flinkcommon.model.SqlCommandCall;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.util.List;

/**
 * 根据sql类型过滤分类sql
 */
public class FilterSqlTypeUtil {
    public static List<SqlCommandCall> filterSqlType(List<SqlCommandCall> sqlCommandCallList, SqlCommand sqlCommand){
        List<SqlCommandCall> filterSqlCommandCallList = Lists.newArrayList();
        for (SqlCommandCall sqlCommandCall : sqlCommandCallList) {
            if(sqlCommand == sqlCommandCall.sqlCommand){
                filterSqlCommandCallList.add(sqlCommandCall);
            }
        }
        return filterSqlCommandCallList;
    }
}
