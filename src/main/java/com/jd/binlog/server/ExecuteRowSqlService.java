package com.jd.binlog.server;

import org.apache.log4j.Logger;
import protocol.protobuf.CanalEntry;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class ExecuteRowSqlService {

    public static final Logger log = Logger.getLogger(ExecuteRowSqlService.class);
    public static final List<String> sqlList = new ArrayList<String>();

    public static void parserAndExecute(CanalEntry.Entry entry,
                                        BinlogPipelineConfig config,
                                        JProxyConnector jProxyConnector){
        if (entry == null) {
            return;
        }
        boolean containFlag = false;
        String tableName = entry.getHeader().getTableName();
        String[] split = config.getTablePrefix().split(",");
        for (String prefix : split) {
            if (tableName.startsWith(prefix)) {
                containFlag = true;
                break;
            }
        }
        if (!containFlag) {
            return;
        }
        if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
            CanalEntry.RowChange rowChage;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
            }
            sqlList.clear();
            CanalEntry.EventType eventType = rowChage.getEventType();
            if (rowChage.getIsDdl()) {
                sqlList.add(rowChage.getSql());
            } else {
                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    String builder = SQLParserBuilder.builder(rowData.getBeforeColumnsList(),
                            rowData.getAfterColumnsList(),
                            eventType,
                            config.getJProxySchema() + "." + config.getJProxyTable());
                    log.info(builder);
                    sqlList.add(builder);
                }
            }
            //execute(sqlList, jProxyConnector);
        }
    }

    public static void execute(List<String> sqlList, JProxyConnector jProxyConnector) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = jProxyConnector.getConnection();
            stmt = conn.createStatement();
            for (String sql : sqlList) {
                stmt.addBatch(sql);
            }
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("execute sql failed. SQLException:" + e.getMessage());
        } finally {
            JProxyConnector.close(null, stmt, conn);
        }
    }
}
