package com.flink.flinksql.flinkcdc;

import com.flink.flinkcommon.catalog.MysqlCatalog;
import com.google.common.collect.Maps;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * flinkcdc整库同步
 */
public class FlinkCdcMultiTableSync03 {

    private static final Logger log = LoggerFactory.getLogger(FlinkCdcMultiTableSync03.class);

    // 本地运行参数
    // --connector_with_body_path /data/flink-study/sql/connector_with_body/hudi_with_body --username root --password 123456789 --host 127.0.0.1 --db flinksql_test
    public static void main(String[] args) throws Exception {

        // 解析参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String userName = parameterTool.get("username");
        String passWord = parameterTool.get("password");
        String host = parameterTool.get("host");
        String db = parameterTool.get("db");
        int port = Integer.valueOf(Optional.ofNullable(parameterTool.get("port")).orElse("3306"));

        // 读取传入参数文件，获取flinksql ddl with参数sql
        String connectorWithBodyPath = parameterTool.get("connector_with_body_path");
        List<String> connectorWithBodyList = Files.readAllLines(Paths.get(connectorWithBodyPath));
        String connectorWithBody = StringUtils.join(connectorWithBodyList, "\n");

        // 创建flink执行环境
        // 本地跑打开web ui
//         StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 注册同步的库对应的catalog
        MysqlCatalog mysqlCatalog = new MysqlCatalog("mysql-catalog", db, userName, passWord, String.format("jdbc:mysql://%s:%d",host,port));
        List<String> tables = mysqlCatalog.listTables(db);

        // 创建表名和对应RowTypeInfo映射的Map
        Map<String, RowTypeInfo> tableTypeInformationMap = Maps.newConcurrentMap();
        Map<String, DataType[]> tableDataTypesMap = Maps.newConcurrentMap();
        for (String table : tables) {
            // 获取mysql catalog中注册的表
            ObjectPath objectPath = new ObjectPath(db, table);
            CatalogBaseTable catalogBaseTable = mysqlCatalog.getTable(objectPath);
            // 获取表的Schema
            TableSchema schema = catalogBaseTable.getSchema();
            // 获取表中字段名列表
            String[] fieldNames = schema.getFieldNames();
            // 获取表的主键
            List<String> primaryKeys = schema.getPrimaryKey().get().getColumns();
            String combinePrimaryKey = StringUtils.join(primaryKeys, ",");
            // 获取表字段类型
            TypeInformation<?>[] fieldTypes = schema.getFieldTypes();
            // 组装sink表ddl sql
            StringBuilder stmt = new StringBuilder();
            String tableName = table.split("\\.")[1];
            String hudiTableName = String.format("hudi_%s", tableName);
            stmt.append("create table ").append(hudiTableName).append("(\n");
            DataType[] fieldDataTypes = schema.getFieldDataTypes();
            for (int i = 0; i < fieldNames.length; i++) {
                String column = fieldNames[i];
                String fieldDataType = fieldDataTypes[i].toString();
                stmt.append("\t").append(column).append(" ").append(fieldDataType);
                if (i < fieldNames.length - 1) {
                    stmt.append(",\n");
                } else {
                    stmt.append("\n) ");
                }
            }
            String formatHudiWithBody = connectorWithBody
                    .replace("${tableName}", hudiTableName)
                    .replace("${recordkeyField}", combinePrimaryKey);
            String createSinkTableDdl = stmt.toString() + formatHudiWithBody;
            // 创建sink表
            log.info("createSinkTableDdl: {}",createSinkTableDdl);
            tEnv.executeSql(createSinkTableDdl);
            tableDataTypesMap.put(tableName,fieldDataTypes);
            tableTypeInformationMap.put(tableName, new RowTypeInfo(fieldTypes, fieldNames));
        }

        // 监控mysql binlog
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("decimal.handling.mode", "string");
        dbzProperties.setProperty("converters","datetime");
        dbzProperties.setProperty("datetime.type","com.flink.flinksql.flinkcdc.CustomerDataConverter");
        dbzProperties.setProperty("datetime.format.date","yyyy-MM-dd");
        dbzProperties.setProperty("datetime.format.time","HH:mm:ss");
        dbzProperties.setProperty("datetime.format.datetime","yyyy-MM-dd HH:mm:ss");
        dbzProperties.setProperty("datetime.format.timestamp","yyyy-MM-dd HH:mm:ss");
        dbzProperties.setProperty("datetime.format.timestamp.zone","UTC+8");
        MySqlSource<Tuple2<String, Row>> mySqlSource = MySqlSource.<Tuple2<String, Row>>builder()
                .debeziumProperties(dbzProperties)
                .hostname(host)
                .port(port)
                .databaseList(db)
                .tableList(String.format("%s.*",db))
                .username(userName)
                .password(passWord)
                .deserializer(new CustomerRowDebeziumDeserializationSchemaV2(tableDataTypesMap))
                .startupOptions(StartupOptions.initial())
                .build();
        SingleOutputStreamOperator<Tuple2<String, Row>> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql cdc").disableChaining();

        //        StatementSet statementSet = tEnv.createStatementSet();
        // dataStream转Table，创建临时视图，插入sink表
        for (Map.Entry<String, RowTypeInfo> entry : tableTypeInformationMap.entrySet()) {
            String tableName = entry.getKey();
            RowTypeInfo rowTypeInfo = entry.getValue();
            SingleOutputStreamOperator<Row> mapStream = dataStreamSource.filter(data -> data.f0.equals(tableName)).map(data -> data.f1, rowTypeInfo);
            Table table = tEnv.fromChangelogStream(mapStream);
            table.printSchema();
//            String temporaryViewName = String.format("t_%s", tableName);
//            tEnv.createTemporaryView(temporaryViewName, table);
//            String sinkTableName = String.format("hudi_%s", tableName);
//            String insertSql = String.format("insert into %s select * from %s", sinkTableName, temporaryViewName);
//            log.info("add insertSql for {},sql: {}",tableName,insertSql);
//            statementSet.addInsertSql(insertSql);
        }
//        statementSet.execute();
        dataStreamSource.print();
        env.execute();
    }
}
