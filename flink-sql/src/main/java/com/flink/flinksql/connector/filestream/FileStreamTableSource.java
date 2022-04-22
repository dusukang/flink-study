package com.flink.flinksql.connector.filestream;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.BulkDecodingFormat;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.filesystem.LimitableBulkFormat;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class FileStreamTableSource implements ScanTableSource {

    // constructor
    private final DynamicTableFactory.Context context;
    private DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat;
    private DecodingFormat<DeserializationSchema<RowData>> deserializationFormat;
    private final FileSystemFormatFactory formatFactory;
    FactoryUtil.TableFactoryHelper helper;

    // connector option
    private String filePath;
    private Integer monitorGap;
    private Boolean isRecursive;
    private Boolean isDatePath;
    private String partitionPattern;
    private String partitionDateFormat;
    private String filePattern;

    private DataType producedDataType;
    private List<ResolvedExpression> filters;

    public FileStreamTableSource(
            DynamicTableFactory.Context context,
            @Nullable DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> deserializationFormat,
            @Nullable FileSystemFormatFactory formatFactory,
            @Nullable FactoryUtil.TableFactoryHelper helper) {
        this.context = context;
        this.bulkReaderFormat = bulkReaderFormat;
        this.deserializationFormat = deserializationFormat;
        this.formatFactory = formatFactory;
        this.helper = helper;
        if (Stream.of(bulkReaderFormat, deserializationFormat, formatFactory)
                .allMatch(Objects::isNull)) {
            Configuration options = Configuration.fromMap(context.getCatalogTable().getOptions());
            String identifier = options.get(FactoryUtil.FORMAT);
            throw new ValidationException(
                    String.format(
                            "Could not find any format factory for identifier '%s' in the classpath.",
                            identifier));
        }
        // get the validated options
        final ReadableConfig options = helper.getOptions();
        filePath = options.get(FileStreamConfigOption.FILEPATH);
        monitorGap = options.get(FileStreamConfigOption.MONITOR_GAP);
        isRecursive = options.get(FileStreamConfigOption.IS_RECURSIVE);
        isDatePath = options.get(FileStreamConfigOption.IS_DATEPATH);
        partitionPattern = options.get(FileStreamConfigOption.PARTITION_PATTERN);
        partitionDateFormat = options.get(FileStreamConfigOption.PARTITION_DATEFORMAT);
        filePattern = options.get(FileStreamConfigOption.FILE_PATTERN);
        // derive the produced data type (excluding computed columns) from the catalog table
        producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {

        if (bulkReaderFormat != null) {
            if (bulkReaderFormat instanceof BulkDecodingFormat
                    && filters != null
                    && filters.size() > 0) {
                ((BulkDecodingFormat<RowData>) bulkReaderFormat).applyFilters(filters);
            }
            BulkFormat<RowData, FileSourceSplit> bulkFormat =
                    bulkReaderFormat.createRuntimeDecoder(scanContext, producedDataType);
            return createSourceProvider(bulkFormat);
        } else if (deserializationFormat != null) {
        final DeserializationSchema<RowData> deserializer = deserializationFormat.createRuntimeDecoder(
                scanContext,
                producedDataType);
        final FileStreamSourceFunction fileStreamSourceFunction = new FileStreamSourceFunction(filePath, monitorGap,isRecursive,isDatePath,partitionPattern,partitionDateFormat,filePattern,deserializer);
        return SourceFunctionProvider.of(fileStreamSourceFunction,false);
        } else {
            throw new TableException("Can not find format factory.");
        }

    }

    SourceProvider createSourceProvider(BulkFormat<RowData, FileSourceSplit> bulkFormat) {
        FileSource.FileSourceBuilder<RowData> builder = FileSource.forBulkFileFormat(
                LimitableBulkFormat.create(bulkFormat, 4096L),
                new Path(filePath));
        return SourceProvider.of(builder.build());
    }

    @Override
    public DynamicTableSource copy() {
        return new FileStreamTableSource(context,bulkReaderFormat,deserializationFormat,formatFactory,helper);
    }

    @Override
    public String asSummaryString() {
        return "file stream read";
    }

}
