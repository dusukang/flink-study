package com.flink.flinksql.connector.filestream;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.calcite.shaded.com.jayway.jsonpath.PathNotFoundException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

public class FileStreamSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
    // 构造参数
    private final String filePath;
    private final Integer monitorGap;
    private final Boolean isRecursive;
    private final Boolean isDatePath;
    private final String partitionPattern;
    private final String partitionDateFormat;
    private final String filePattern;
    private final DeserializationSchema<RowData> deserializer;
    // 生成参数
    private Path path;
    private Map<String, Set<String>> cacheDayFiles = new HashMap<>();
    private volatile Boolean isRunning = true;
    private FileSystem fs;
    private static final DateTimeFormatter DATE_FORMATTER_NUMBER = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter DATE_FORMATTER_HOUR = DateTimeFormatter.ofPattern("HH");
    private List<DateTimeFormatter> dateTimeFormatters = new ArrayList<>();

    public FileStreamSourceFunction(String filePath, Integer monitorGap, Boolean isRecursive, Boolean isDatePath, String partitionPattern, String partitionDateFormat, String filePattern, DeserializationSchema<RowData> deserializer) {
        this.filePath = filePath;
        this.monitorGap = monitorGap;
        this.isRecursive = isRecursive;
        this.isDatePath = isDatePath;
        this.partitionPattern = partitionPattern;
        this.partitionDateFormat = partitionDateFormat;
        this.filePattern = filePattern;
        this.deserializer = deserializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        path = new Path(filePath);
        // 初始化文件
        fs = FileSystem.get(new URI(filePath));
        // 处理日期格式的路径
        String[] pathNames = partitionDateFormat.split("/");
        for (String pathName : pathNames) {
            dateTimeFormatters.add(DateTimeFormatter.ofPattern(pathName));
        }
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        while (isRunning) {
            // 定时加载文件
            if (fs.exists(path)) {
                if (isDatePath) {
                    LocalDateTime nowDateTime = LocalDateTime.now();
                    String nowDate = nowDateTime.format(DATE_FORMATTER_NUMBER);
                    String nowHour = nowDateTime.format(DATE_FORMATTER_HOUR);
                    // 判断是否扫描近两日文件
                    if (!cacheDayFiles.containsKey(nowDate) || nowHour.equals("00")) {
                        LocalDateTime lastDateTime = nowDateTime.plusDays(-1);
                        for (LocalDateTime dateTime : new LocalDateTime[]{lastDateTime, nowDateTime}) {
                            String datePath = filePath;
                            for (DateTimeFormatter dateTimeFormatter : dateTimeFormatters) {
                                datePath = datePath + "/" + partitionPattern + dateTime.format(dateTimeFormatter);
                            }
                            monitorDatePartitionFilepath(fs, dateTime.format(DATE_FORMATTER_NUMBER),new Path(datePath), isRecursive, sourceContext);
                        }
                    } else {
                        String datePath = filePath;
                        for (DateTimeFormatter dateTimeFormatter : dateTimeFormatters) {
                            datePath = datePath + "/" + partitionPattern + nowDateTime.format(dateTimeFormatter);
                        }
                        monitorDatePartitionFilepath(fs, nowDate,new Path(datePath), isRecursive, sourceContext);
                    }
                } else {
                    monitorFilePath(fs, new Path(filePath), isRecursive, sourceContext);
                }
            } else {
                throw new PathNotFoundException(String.format("%s:路径不存在", filePath));
            }
            Thread.sleep(monitorGap);
        }
    }

    /**
     * 监控非日期格式路径下文件
     *
     * @param fs
     * @param filePath
     * @param isRecursive
     * @param sourceContext
     * @throws IOException
     */
    public void monitorFilePath(FileSystem fs, Path filePath, Boolean isRecursive, SourceContext<RowData> sourceContext) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(filePath);
        if(null == fileStatuses || fileStatuses.length == 0){
            return;
        }
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDir() && isRecursive) {
                monitorFilePath(fs, fileStatus.getPath(), isRecursive, sourceContext);
                continue;
            }
            String fileName = fileStatus.getPath().getName();
            if (!cacheDayFiles.containsKey(fileName) && Pattern.matches(filePattern, fileName)) {
                FSDataInputStream inputStream = fs.open(new Path(filePath + "/" + fileName));
                List<String> lines = IOUtils.readLines(inputStream, "utf-8");
                for (String line : lines) {
                    sourceContext.collect(deserializer.deserialize(line.getBytes()));
                }
                inputStream.close();
                cacheDayFiles.put(fileName,null);
            }
        }
    }

    /**
     * 监控日期格式分区路径下文件
     *
     * @param fs
     * @param filePath
     * @param isRecursive
     * @param sourceContext
     * @throws IOException
     */
    public void monitorDatePartitionFilepath(FileSystem fs, String dateTag,Path filePath, Boolean isRecursive, SourceContext<RowData> sourceContext) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(filePath);
        if(null == fileStatuses || fileStatuses.length == 0){
            return;
        }
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDir()) {
                if (isRecursive) {
                    monitorDatePartitionFilepath(fs, dateTag,fileStatus.getPath(), isRecursive, sourceContext);
                    continue;
                }
            } else {
                Set<String> cacheFiles = Optional.ofNullable(cacheDayFiles.get(dateTag)).orElse(new HashSet<>());
                String fileName = fileStatus.getPath().getName();
                if (!cacheFiles.contains(fileName) && Pattern.matches(filePattern, fileName)) {
                    FSDataInputStream inputStream = fs.open(new Path(filePath + "/" + fileName));
                    List<String> lines = IOUtils.readLines(inputStream, "utf-8");
                    for (String line : lines) {
                        sourceContext.collect(deserializer.deserialize(line.getBytes()));
                    }
                    inputStream.close();
                    cacheFiles.add(fileName);
                    cacheDayFiles.put(dateTag, cacheFiles);
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializer.getProducedType();
    }
}
