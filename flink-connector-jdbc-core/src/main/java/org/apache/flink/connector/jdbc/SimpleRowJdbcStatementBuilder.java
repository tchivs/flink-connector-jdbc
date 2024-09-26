package org.apache.flink.connector.jdbc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

/** Simple implementation of {@link JdbcStatementBuilder} for {@link Row}. */
public class SimpleRowJdbcStatementBuilder implements JdbcStatementBuilder<Row> {

    private final TypeInformation<?>[] fieldTypes;

    private SimpleRowJdbcStatementBuilder(TypeInformation<?>[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    @Override
    public void accept(PreparedStatement preparedStatement, Row row) throws SQLException {
        if (row.getArity() != fieldTypes.length) {
            throw new SQLException("Row arity does not match TypeInformation arity");
        }

        for (int i = 0; i < row.getArity(); i++) {
            Object field = row.getField(i);
            if (field == null) {
                preparedStatement.setNull(i + 1, java.sql.Types.NULL);
            } else {
                setField(preparedStatement, i + 1, field, fieldTypes[i]);
            }
        }
    }

    private void setField(PreparedStatement ps, int index, Object value, TypeInformation<?> type)
            throws SQLException {
        if (type == Types.STRING) {
            ps.setString(index, (String) value);
        } else if (type == Types.INT) {
            ps.setInt(index, (Integer) value);
        } else if (type == Types.LONG) {
            ps.setLong(index, (Long) value);
        } else if (type == Types.FLOAT) {
            ps.setFloat(index, (Float) value);
        } else if (type == Types.DOUBLE) {
            ps.setDouble(index, (Double) value);
        } else if (type == Types.BOOLEAN) {
            ps.setBoolean(index, (Boolean) value);
        } else if (type == Types.SQL_DATE) {
            ps.setDate(index, (Date) value);
        } else if (type == Types.SQL_TIME) {
            ps.setTime(index, (Time) value);
        } else if (type == Types.SQL_TIMESTAMP) {
            ps.setTimestamp(index, (Timestamp) value);
        } else if (type == Types.BIG_DEC) {
            ps.setBigDecimal(index, (BigDecimal) value);
        } else if (type == Types.BYTE) {
            ps.setByte(index, (Byte) value);
        } else if (type == Types.SHORT) {
            ps.setShort(index, (Short) value);
        } else {
            // For other types, try to set as object and let JDBC driver handle it
            ps.setObject(index, value);
        }
    }

    public static SimpleRowJdbcStatementBuilder create(TypeInformation<?>... typeInformation) {
        return new SimpleRowJdbcStatementBuilder(typeInformation);
    }
}
