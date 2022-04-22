package com.flink.flinkstream.flinkcdc;

import com.flink.flinkcommon.model.SqlCommandCall;
import com.flink.flinkcommon.model.SqlJobRunParam;
import com.flink.flinkcommon.sql.SqlFileParser;
import com.flink.flinkcommon.util.ParamUtil;
import com.google.common.collect.Maps;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FlinkCdcMultyTableSync {
    private static final Logger log = LoggerFactory.getLogger(FlinkCdcMultyTableSync.class);
    static Map<String, RowTypeInfo> tableTypeInformationMap = Maps.newHashMap();
    public static void main(String[] args) throws Exception {
        Arrays.stream(args).forEach(arg -> log.warn(arg));
        SqlJobRunParam jobRunParam = ParamUtil.buildSqlJobParam(args);
        List<String> sql = Files.readAllLines(Paths.get(jobRunParam.getSqlPath()));
        List<SqlCommandCall> sqlCommandCallList = SqlFileParser.fileToSql(sql);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        for (SqlCommandCall sqlCommandCall : sqlCommandCallList) {
            tEnv.executeSql(sqlCommandCall.operands[0]);
        }
        String[] defaultTables = tEnv.listTables();
        for (String defaultTable : defaultTables) {
            ObjectPath objectPath = new ObjectPath("default_database", defaultTable);
            CatalogBaseTable catalogBaseTable = tEnv.getCatalog("default_catalog").get().getTable(objectPath);
            TableSchema schema = catalogBaseTable.getSchema();
            catalogBaseTable.getUnresolvedSchema().getColumns();
            for (Schema.UnresolvedColumn column : catalogBaseTable.getUnresolvedSchema().getColumns()) {
                System.out.println(column.getName());
            }
            TypeInformation<?>[] fieldTypes = schema.getFieldTypes();
            for (TypeInformation<?> fieldType : fieldTypes) {
                System.out.println(fieldType);
            }
            tableTypeInformationMap.put(defaultTable, new RowTypeInfo(fieldTypes));
        }
        tableTypeInformationMap.forEach(
                (k,v)->{
                    System.out.println(k);
                    System.out.println(v);
                }
        );
        MySqlSource<Tuple2<String, Row>> mySqlSource = MySqlSource.<Tuple2<String, Row>>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("flinksql_test")
                .tableList("flinksql_test.*")
                .username("root")
                .password("")
                .deserializer(new CustomerRowDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<Tuple2<String, Row>> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql cdc");
        SingleOutputStreamOperator<Row> process = dataStreamSource.process(new ProcessFunction<Tuple2<String, Row>, Row>() {
            @Override
            public void processElement(Tuple2<String, Row> value, Context ctx, Collector<Row> out) throws Exception {
                if (value.f0.equals("order")) {
                    out.collect(value.f1);
                }
            }
        }, tableTypeInformationMap.get("order"));
        process.print();
        //        for (Map.Entry<String, RowTypeInfo> entry : tableTypeInformationMap.entrySet()) {
//            SingleOutputStreamOperator<Row> process =
//                    dataStreamSource.process(
//                            new ProcessFunction<Tuple2<String, Row>, Row>() {
//                                @Override
//                                public void processElement(Tuple2<String, Row> value, Context ctx, Collector<Row> out) throws Exception {
//                                    if (value.f0.equals(entry.getKey())) {
//                                        out.collect(value.f1);
//                                    }
//                                }
//                            }, tableTypeInformationMap.get(entry.getKey())
//                    );
//            process.print();
//        }
        env.execute();
    }
}
