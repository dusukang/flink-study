package com.flink.flinksql.flinkcdc;

import com.flink.flinksql.enums.TableRowTypeInfoEnum;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkCdcMultiTableSync01 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        MySqlSource<Tuple2<String,Row>> mySqlSource = MySqlSource.<Tuple2<String,Row>>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("flinksql_test")
                .tableList("flinksql_test.*")
                .username("root")
                .password("123456789")
                .deserializer(new CustomerRowDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        SingleOutputStreamOperator<Tuple2<String, Row>> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql cdc").disableChaining();

        // order表
        SingleOutputStreamOperator<Tuple2<String, Row>> orderStream = dataStreamSource.filter(item -> item.f0.equals("order"));
        SingleOutputStreamOperator<Row> orderMapStream = orderStream.map(item -> item.f1, TableRowTypeInfoEnum.tableRowTyeInfoMap.get("order").f1);
        Table tableOrder = tEnv.fromChangelogStream(orderMapStream).as("order_id","order_type","cost","order_name");

        // order_product表
        SingleOutputStreamOperator<Tuple2<String, Row>> orderProductStream = dataStreamSource.filter(item -> item.f0.equals("order_product"));
        SingleOutputStreamOperator<Row> orderProductMapStream = orderProductStream.map(item -> item.f1, TableRowTypeInfoEnum.tableRowTyeInfoMap.get("order_product").f1);
        Table tableOrderProduct = tEnv.fromChangelogStream(orderProductMapStream).as("pro_id","pro_name","pro_type","order_id","order_type","order_name","area","cost");

        // project表
        SingleOutputStreamOperator<Tuple2<String, Row>> projectStream = dataStreamSource.filter(item -> item.f0.equals("project"));
        SingleOutputStreamOperator<Row> projectMapStream = projectStream.map(item -> item.f1, TableRowTypeInfoEnum.tableRowTyeInfoMap.get("project").f1);
        Table tableProject = tEnv.fromChangelogStream(projectMapStream).as("pro_id","pro_name","total_score");

        // student_project表
        SingleOutputStreamOperator<Tuple2<String, Row>> studentProjectStream = dataStreamSource.filter(item -> item.f0.equals("student_project"));
        SingleOutputStreamOperator<Row> studentProjectMapStream = studentProjectStream.map(item -> item.f1, TableRowTypeInfoEnum.tableRowTyeInfoMap.get("student_project").f1);
        Table tableStudentProject = tEnv.fromChangelogStream(studentProjectMapStream).as("stu_id","stu_name","pro_id","pro_name","score","total_score");

        // user表
        SingleOutputStreamOperator<Tuple2<String, Row>> userStream = dataStreamSource.filter(item -> item.f0.equals("user"));
        SingleOutputStreamOperator<Row> userMapStream = userStream.map(item -> item.f1, TableRowTypeInfoEnum.tableRowTyeInfoMap.get("user").f1);
        Table tableUser = tEnv.fromChangelogStream(userMapStream).as("uid","uname","others");

        // user_login_times表
        SingleOutputStreamOperator<Tuple2<String, Row>> userLoginTimesStream = dataStreamSource.filter(item -> item.f0.equals("user_login_times"));
        SingleOutputStreamOperator<Row> userLoginTimesMapStream = userLoginTimesStream.map(new MapFunction<Tuple2<String, Row>, Row>() {
            @Override
            public Row map(Tuple2<String, Row> value) throws Exception {
                System.out.println(value);
                return value.f1;
            }
        }, TableRowTypeInfoEnum.tableRowTyeInfoMap.get("user_login_times").f1);
        Table tableUserLoginTimes = tEnv.fromChangelogStream(userLoginTimesMapStream).as("uid","window_start","window_end","login_times");
        tableUserLoginTimes.printSchema();

        tEnv.createTemporaryView("source_cdc_order",tableOrder);
        tEnv.createTemporaryView("source_cdc_order_product",tableOrderProduct);
        tEnv.createTemporaryView("source_cdc_project",tableProject);
        tEnv.createTemporaryView("source_cdc_student_project",tableStudentProject);
        tEnv.createTemporaryView("source_cdc_user",tableUser);
        tEnv.createTemporaryView("source_cdc_user_login_times",tableUserLoginTimes);

        tEnv.executeSql("CREATE TABLE sink_jdbc_order(\n" +
                "    order_id bigint,\n" +
                "    order_type int,\n" +
                "    cost decimal(16,2),\n" +
                "    order_name varchar,\n" +
                "    PRIMARY KEY (order_id) NOT ENFORCED\n" +
                ")with(\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test_bak01?useSSL=false',\n" +
                "   'table-name' = 'order',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456789'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE sink_jdbc_order_product(\n" +
                "    pro_id bigint,\n" +
                "    pro_name varchar,\n" +
                "    pro_type int,\n" +
                "    order_id bigint,\n" +
                "    order_type int,\n" +
                "    order_name varchar,\n" +
                "    area varchar,\n" +
                "    cost decimal(16,2),\n" +
                "    PRIMARY KEY (pro_id) NOT ENFORCED\n" +
                ")with(\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test_bak01?useSSL=false',\n" +
                "   'table-name' = 'order_product',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456789'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE sink_jdbc_project(\n" +
                "    pro_id bigint,\n" +
                "    pro_name varchar,\n" +
                "    total_score decimal(10,2),\n" +
                "    PRIMARY KEY (pro_id) NOT ENFORCED\n" +
                ")with(\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test_bak01?useSSL=false',\n" +
                "   'table-name' = 'project',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456789'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE sink_jdbc_student_project(\n" +
                "    stu_id bigint,\n" +
                "    stu_name varchar,\n" +
                "    pro_id bigint,\n" +
                "    pro_name varchar,\n" +
                "    score decimal(10,2),\n" +
                "    total_score decimal(10,2),\n" +
                "    PRIMARY KEY (stu_id) NOT ENFORCED\n" +
                ")with(\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test_bak01?useSSL=false',\n" +
                "   'table-name' = 'student_project',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456789'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE sink_jdbc_user(\n" +
                "    uid bigint,\n" +
                "    uname varchar,\n" +
                "    others varchar,\n" +
                "    PRIMARY KEY (uid) NOT ENFORCED\n" +
                ")with(\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test_bak01?useSSL=false',\n" +
                "   'table-name' = 'user',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456789'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE sink_jdbc_user_login_times(\n" +
                "    uid bigint,\n" +
                "    window_start timestamp,\n" +
                "    window_end timestamp,\n" +
                "    login_times bigint,\n" +
                "    PRIMARY KEY (uid,window_start,window_end) NOT ENFORCED\n" +
                ")with(\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://127.0.0.1:3306/flinksql_test_bak01?useSSL=false',\n" +
                "   'table-name' = 'user_login_times',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456789'\n" +
                ")");

        StatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql("insert into sink_jdbc_order select * from source_cdc_order");
        statementSet.addInsertSql("insert into sink_jdbc_order_product select * from source_cdc_order_product");
        statementSet.addInsertSql("insert into sink_jdbc_project select * from source_cdc_project");
        statementSet.addInsertSql("insert into sink_jdbc_student_project select * from source_cdc_student_project");
        statementSet.addInsertSql("insert into sink_jdbc_user select * from source_cdc_user");
        statementSet.addInsertSql("insert into sink_jdbc_user_login_times select * from source_cdc_user_login_times");
        statementSet.execute();

    }
}
