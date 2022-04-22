package com.flink.flinksql.connector.filestream;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;

import java.util.*;
import java.util.stream.Collectors;

public class FileStreamTableFactory implements DynamicTableSourceFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // discover decoding format
        DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkFormatDecodingFormat = discoverDecodingFormat(context, helper,BulkReaderFormatFactory.class);
        DecodingFormat<DeserializationSchema<RowData>> deserializationSchemaDecodingFormat = discoverDecodingFormat(context,helper,DeserializationFormatFactory.class);

        // validate all options
        helper.validate();

        return new FileStreamTableSource(
                context,
                bulkFormatDecodingFormat,
                deserializationSchemaDecodingFormat,
                discoverFormatFactory(context),
                helper);
    }

    @Override
    public String factoryIdentifier() {
        return "filestream";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FileStreamConfigOption.FILEPATH);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FileStreamConfigOption.MONITOR_GAP);
        options.add(FileStreamConfigOption.IS_RECURSIVE);
        options.add(FileStreamConfigOption.IS_DATEPATH);
        options.add(FileStreamConfigOption.PARTITION_PATTERN);
        options.add(FileStreamConfigOption.PARTITION_DATEFORMAT);
        options.add(FileStreamConfigOption.FILE_PATTERN);
        return options;
    }

    private FileSystemFormatFactory discoverFormatFactory(Context context) {
        if (formatFactoryExists(context, FileSystemFormatFactory.class)) {
            Configuration options = Configuration.fromMap(context.getCatalogTable().getOptions());
            String identifier = options.get(FactoryUtil.FORMAT);
            return FactoryUtil.discoverFactory(
                    Thread.currentThread().getContextClassLoader(),
                    FileSystemFormatFactory.class,
                    identifier);
        } else {
            return null;
        }
    }

    private <I, F extends DecodingFormatFactory<I>> DecodingFormat<I> discoverDecodingFormat(
            Context context,FactoryUtil.TableFactoryHelper helper,Class<F> formatFactoryClass) {
        if (formatFactoryExists(context, formatFactoryClass)) {
            return helper.discoverDecodingFormat(formatFactoryClass, FactoryUtil.FORMAT);
        } else {
            return null;
        }
    }

    /**
     * Returns true if the format factory can be found using the given factory base class and
     * identifier.
     */
    private boolean formatFactoryExists(Context context, Class<?> factoryClass) {
        Configuration options = Configuration.fromMap(context.getCatalogTable().getOptions());
        String identifier = options.get(FactoryUtil.FORMAT);
        if (identifier == null) {
            throw new ValidationException(
                    String.format(
                            "Table options do not contain an option key '%s' for discovering a format.",
                            FactoryUtil.FORMAT.key()));
        }

        final List<Factory> factories = new LinkedList<>();
        ServiceLoader.load(Factory.class, context.getClassLoader())
                .iterator()
                .forEachRemaining(factories::add);

        final List<Factory> foundFactories =
                factories.stream()
                        .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                        .collect(Collectors.toList());

        final List<Factory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equals(identifier))
                        .collect(Collectors.toList());

        return !matchingFactories.isEmpty();
    }

}
