package com.jd.binlog.server;

import protocol.protobuf.CanalEntry;

import java.sql.Types;
import java.util.List;

/**
 * Created by guoliming on 2015/6/23.
 */
public class SQLParserBuilder {
    public static String INSERT_ENTRY = "INSERT INTO ";
    public static String UPDATE_ENTRY = "UPDATE ";
    public static String DELETE_ENTRY = "DELETE FROM ";

    public static String builder(List<CanalEntry.Column> beforeColumns,
                          List<CanalEntry.Column> afterColumns,
                          CanalEntry.EventType eventType,
                          String fullTableName) {
        StringBuffer sqlBuilder = new StringBuffer();
        if (eventType == CanalEntry.EventType.DELETE) {
            sqlBuilder.append(DELETE_ENTRY);
            sqlBuilder.append(fullTableName);
            sqlBuilder.append(" WHERE ");
            boolean addAndPartFlag = false;
            for (int i = 0; i < beforeColumns.size(); i++) {
                appendWherePart(sqlBuilder, beforeColumns.get(i), addAndPartFlag);
                addAndPartFlag = true;
            }
        } else if (eventType == CanalEntry.EventType.INSERT) {
            sqlBuilder.append(INSERT_ENTRY);
            sqlBuilder.append(fullTableName);
            sqlBuilder.append(" VALUES (");
            for (int i = 0; i < afterColumns.size(); i++) {
                appendUpdateValue(sqlBuilder, afterColumns.get(i), i, afterColumns.size());
            }
            sqlBuilder.append(" )");
        } else if (eventType == CanalEntry.EventType.UPDATE) {
            sqlBuilder.append(UPDATE_ENTRY);
            sqlBuilder.append(fullTableName);
            sqlBuilder.append(" SET ");
            StringBuffer afterPart = new StringBuffer();
            StringBuffer beforePart = new StringBuffer();
            boolean addAndPartFlag = false;
            for (int i = 0; i < afterColumns.size(); i++) {
                afterPart.append(afterColumns.get(i).getName());
                afterPart.append(" = ");
                appendUpdateValue(afterPart, afterColumns.get(i), i, afterColumns.size());
                appendWherePart(beforePart, beforeColumns.get(i), addAndPartFlag);
                addAndPartFlag = true;
            }
            sqlBuilder.append(afterPart);
            sqlBuilder.append(" WHERE ");
            sqlBuilder.append(beforePart);
        }
        return sqlBuilder.toString();
    }

    public static void appendUpdateValue(StringBuffer sqlBuilder, CanalEntry.Column column, int index, int columnSize) {
        int sqlType = column.getSqlType();
        String value = column.getValue();
        appendValueByType(sqlType, value, sqlBuilder);
        if (index < columnSize - 1) {
            sqlBuilder.append(", ");
        }
    }

    public static void appendWherePart(StringBuffer sqlBuilder, CanalEntry.Column column, boolean AddAndPartFlag) {
        if (AddAndPartFlag) {
            sqlBuilder.append(" AND ");
        }
        sqlBuilder.append(column.getName());
        sqlBuilder.append(" = ");
        int sqlType = column.getSqlType();
        String value = column.getValue();
        appendValueByType(sqlType, value, sqlBuilder);
    }

    public static void appendValueByType(int sqlType, String value, StringBuffer sqlBuilder) {
        switch (sqlType) {
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.BIGINT:
            case Types.REAL: // float
            case Types.DOUBLE: // double
            case Types.BIT:// bit
            case Types.DECIMAL:
                sqlBuilder.append(value);
                break;
            default:
                sqlBuilder.append("'" + value + "'");
        }
    }
}
