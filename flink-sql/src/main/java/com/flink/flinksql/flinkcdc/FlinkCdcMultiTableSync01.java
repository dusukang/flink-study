package com.flink.flinksql.flinkcdc;

import com.flink.flinkcommon.enums.SqlCommand;
import com.flink.flinkcommon.execute.ExecuteSql;
import com.flink.flinkcommon.model.SqlCommandCall;
import com.flink.flinkcommon.model.SqlJobRunParam;
import com.flink.flinkcommon.sql.SqlFileParser;
import com.flink.flinkcommon.util.FilterSqlTypeUtil;
import com.flink.flinkcommon.util.ParamUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FlinkCdcMultiTableSync01 {
    private static final Logger log = LoggerFactory.getLogger(FlinkCdcMultiTableSync01.class);
    public static void main(String[] args) throws Exception {
        Map<String, RowTypeInfo> tableTypeInformationMap = Maps.newConcurrentMap();
        Arrays.stream(args).forEach(arg -> log.warn(arg));
        SqlJobRunParam jobRunParam = ParamUtil.buildSqlJobParam(args);
        List<String> sql = Files.readAllLines(Paths.get(jobRunParam.getSqlPath()));
        List<SqlCommandCall> sqlCommandCallList = SqlFileParser.fileToSql(sql);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.enableCheckpointing(3000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        StatementSet statementSet = tEnv.createStatementSet();
        List<SqlCommandCall> createSqlCommandCallList = FilterSqlTypeUtil.filterSqlType(sqlCommandCallList, SqlCommand.CREATE_TABLE);
        ExecuteSql.exeSql(createSqlCommandCallList,tEnv,statementSet);
        String[] defaultTables = tEnv.listTables();
        for (String defaultTable : defaultTables) {
            // 获取catalog中注册的表
            ObjectPath objectPath = new ObjectPath("default_database", defaultTable);
            CatalogBaseTable catalogBaseTable = tEnv.getCatalog("default_catalog").get().getTable(objectPath);
            List<String> columnList = Lists.newArrayList();
            // 获取表中字段名列表
            for (Schema.UnresolvedColumn column : catalogBaseTable.getUnresolvedSchema().getColumns()) {
                columnList.add(column.getName());
            }
            String[] columns = columnList.toArray(new String[]{});
            // 获取表中字段类型
            TableSchema schema = catalogBaseTable.getSchema();
            TypeInformation<?>[] fieldTypes = schema.getFieldTypes();
            tableTypeInformationMap.put(defaultTable, new RowTypeInfo(fieldTypes, columns));
        }
        MySqlSource<Tuple2<String,Row>> mySqlSource = MySqlSource.<Tuple2<String,Row>>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("flinksql_test")
                .tableList("flinksql_test.*")
                .username("root")
                .password("")
                .deserializer(new CustomerRowDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        SingleOutputStreamOperator<Tuple2<String, Row>> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql cdc").disableChaining();

        for (Map.Entry<String, RowTypeInfo> entry : tableTypeInformationMap.entrySet()) {
            String key = entry.getKey();
            RowTypeInfo rowTypeInfo = entry.getValue();
            SingleOutputStreamOperator<Row> mapStream = dataStreamSource.filter(data -> data.f0.equals(key)).map(data -> data.f1, rowTypeInfo);
            Table table = tEnv.fromChangelogStream(mapStream);
            tEnv.createTemporaryView(String.format("t_%s",key),table);
        }
        List<SqlCommandCall> insertSqlCommandCallList = FilterSqlTypeUtil.filterSqlType(sqlCommandCallList, SqlCommand.INSERT_INTO);
        ExecuteSql.exeSql(insertSqlCommandCallList,tEnv,statementSet);
        statementSet.execute();
    }
}
