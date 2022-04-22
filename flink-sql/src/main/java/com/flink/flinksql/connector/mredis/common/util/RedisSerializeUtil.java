package com.flink.flinksql.connector.mredis.common.util;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import java.math.BigDecimal;
import java.util.Base64;

/** redis serialize . */
public class RedisSerializeUtil {

    public static Object dataTypeFromString(LogicalType fieldType, String result) {
        switch (fieldType.getTypeRoot()) {
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return Long.valueOf(result);
            case FLOAT:
                return Float.valueOf(result);
            case DOUBLE:
                return Double.valueOf(result);
            case CHAR:
            case VARCHAR:
                return BinaryStringData.fromString(result);
            case BOOLEAN:
                return Boolean.valueOf(result);
            case BINARY:
            case VARBINARY:
                return Base64.getDecoder().decode(result);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                final int precision = decimalType.getPrecision();
                final int scale = decimalType.getScale();
                BigDecimal decimal = new BigDecimal(result);
                return DecimalData.fromBigDecimal(decimal, precision, scale);
            case TINYINT:
                return Byte.valueOf(result);
            case SMALLINT:
                return Short.valueOf(result);
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
                return Integer.valueOf(result);
            default:
                throw new RuntimeException("Unsupported field type: " + fieldType);
        }
    }

    public static String rowDataToString(LogicalType fieldType, RowData rowData, Integer index) {
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return rowData.getString(index).toString();
            case BOOLEAN:
                return String.valueOf(rowData.getBoolean(index));
            case BINARY:
            case VARBINARY:
                return Base64.getEncoder().encodeToString(rowData.getBinary(index));
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                final int precision = decimalType.getPrecision();
                final int scale = decimalType.getScale();
                BigDecimal decimal = rowData.getDecimal(index, precision, scale).toBigDecimal();
                return decimal.toString();
            case TINYINT:
                return String.valueOf(rowData.getByte(index));
            case SMALLINT:
                return String.valueOf(rowData.getString(index));
            case INTEGER:
            case DATE:
                return String.valueOf(rowData.getInt(index));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return String.valueOf(rowData.getLong(index));
            case FLOAT:
                return String.valueOf(rowData.getFloat(index));
            case DOUBLE:
                return String.valueOf(rowData.getDouble(index));
            default:
                throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
        }
    }

    public static Object rowDataToObject(LogicalType fieldType, RowData rowData, Integer index) {
        switch (fieldType.getTypeRoot()) {
            case CHAR:
                return rowData.getString(index).toString();
            case VARCHAR:
                return rowData.getString(index).toString();
            case BOOLEAN:
                return rowData.getBoolean(index);
            case BINARY:
                return rowData.getBinary(index);
            case VARBINARY:
                return rowData.getBinary(index);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                final int precision = decimalType.getPrecision();
                final int scale = decimalType.getScale();
                return rowData.getDecimal(index, precision, scale).toBigDecimal();
            case TINYINT:
                return rowData.getByte(index);
            case SMALLINT:
                return rowData.getShort(index);
            case INTEGER:
                return rowData.getInt(index);
            case DATE:
                return rowData.getInt(index);
            case BIGINT:
                return rowData.getLong(index);
            case INTERVAL_DAY_TIME:
                return rowData.getLong(index);
            case FLOAT:
                return rowData.getFloat(index);
            case DOUBLE:
                return rowData.getDouble(index);
            default:
                throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
        }
    }

}
