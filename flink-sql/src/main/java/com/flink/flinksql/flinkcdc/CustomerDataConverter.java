package com.flink.flinksql.flinkcdc;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.function.Consumer;

@Slf4j
public class CustomerDataConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;
    private DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME;
    private DateTimeFormatter datetimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
    private DateTimeFormatter timestampFormatter = DateTimeFormatter.ISO_DATE_TIME;

    private ZoneId timestampZoneId = ZoneId.systemDefault();

    @Override
    public void configure(Properties props) {
        readProps(props, "format.date", p -> dateFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.time", p -> timeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.datetime", p -> datetimeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.timestamp", p -> timestampFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.timestamp.zone", z -> timestampZoneId = ZoneId.of(z));
    }

    private void readProps(Properties properties, String settingKey, Consumer<String> callback) {
        String settingValue = (String) properties.get(settingKey);
        if (settingValue == null || settingValue.length() == 0) {
            return;
        }
        try {
            callback.accept(settingValue.trim());
        } catch (IllegalArgumentException | DateTimeException e) {
            log.error("The \"{}\" setting is illegal:{}", settingKey, settingValue);
            throw e;
        }
    }

    @SneakyThrows
    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = field.typeName().toUpperCase();
        SchemaBuilder schemaBuilder = null;
        Converter converter = null;
        if ("DATE".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("DATE");
            converter = this::convertDate;
        }
        if ("TIME".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("TIME");
            converter = this::convertTime;
        }
        if ("DATETIME".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("DATETIME");
            converter = this::convertDateTime;
        }
        if ("TIMESTAMP".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("TIMESTAMP");
            converter = this::convertTimestamp;
        }
        if ("FLOAT".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("FLOAT");
            converter = this::convertFloat;
        }
        if (schemaBuilder != null) {
            registration.register(schemaBuilder, converter);
            log.info("register converter for sqlType {} to schema {}", sqlType, schemaBuilder.name());
        }
    }

    private String convertDate(Object input) {
        if (input instanceof LocalDate) {
            return dateFormatter.format((LocalDate) input);
        }
        if (input instanceof Integer) {
            LocalDate date = LocalDate.ofEpochDay((Integer) input);
            return dateFormatter.format(date);
        }
        return null;
    }

    private String convertTime(Object input) {
        if (input instanceof Duration) {
            Duration duration = (Duration) input;
            long seconds = duration.getSeconds();
            int nano = duration.getNano();
            LocalTime time = LocalTime.ofSecondOfDay(seconds).withNano(nano);
            return timeFormatter.format(time);
        }
        return null;
    }

    private String convertDateTime(Object input) {
        if (input instanceof Timestamp) {
            return datetimeFormatter.format(((Timestamp) input).toLocalDateTime());
        }
        return null;
    }

    private String convertTimestamp(Object input) {
        if (input instanceof Timestamp) {
            LocalDateTime localDateTime = ((Timestamp) input).toLocalDateTime();
            ZonedDateTime zonedDateTime = localDateTime.atZone(timestampZoneId).withZoneSameInstant(ZoneId.from(ZoneOffset.UTC));
            return timestampFormatter.format(zonedDateTime.toLocalDateTime());
        }
        if(input instanceof ZonedDateTime){
            ZonedDateTime zonedDateTime = ((ZonedDateTime)input).withZoneSameInstant(ZoneId.from(ZoneOffset.ofHours(8)));
            return timestampFormatter.format(zonedDateTime.toLocalDateTime());
        }
        return null;
    }

    private Object convertFloat(Object input) {
        return String.valueOf(input);
    }
}
