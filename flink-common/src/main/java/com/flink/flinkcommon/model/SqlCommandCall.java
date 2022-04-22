package com.flink.flinkcommon.model;

import com.flink.flinkcommon.enums.SqlCommand;
import lombok.Data;

@Data
public class SqlCommandCall {

    public SqlCommand sqlCommand;

    public String[] operands;

    public SqlCommandCall(SqlCommand sqlCommand, String[] operands) {
        this.sqlCommand = sqlCommand;
        this.operands = operands;
    }

    public SqlCommandCall(String[] operands) {
        this.operands = operands;
    }
}
