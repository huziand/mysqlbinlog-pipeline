package com.jd.binlog.server.common;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.jd.binlog.connection.MysqlConnector;
import com.jd.binlog.connection.MysqlQueryExecutor;
import com.jd.binlog.connection.packets.server.FieldPacket;
import com.jd.binlog.connection.packets.server.ResultSetPacket;
import com.jd.binlog.server.common.TableMeta.FieldMeta;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by hp on 14-9-3.
 */
public class TableMetaCache {

    private static final Logger log = Logger.getLogger(TableMetaCache.class);
    public static final String     COLUMN_NAME    = "COLUMN_NAME";
    public static final String     COLUMN_TYPE    = "COLUMN_TYPE";
    public static final String     IS_NULLABLE    = "IS_NULLABLE";
    public static final String     COLUMN_KEY     = "COLUMN_KEY";
    public static final String     COLUMN_DEFAULT = "COLUMN_DEFAULT";
    public static final String     EXTRA          = "EXTRA";
    private MysqlConnector         connector;

    // 第一层tableId,第二层schema.table,解决tableId重复，对应多张表
    private LoadingCache<String, TableMeta> tableMetaCache;

    public TableMetaCache(MysqlConnector coner){
        this.connector = coner;
        tableMetaCache = CacheBuilder.newBuilder()
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .refreshAfterWrite(5, TimeUnit.MINUTES)
                .build(new CacheLoader<String, TableMeta>() {
                    @Override
                    public TableMeta load(String s) throws Exception {
                        return getTableMeta0(s);
                    }
                });
    }

    public TableMeta getTableMeta(String schema, String table) {
        try {
            return tableMetaCache.get(getFullName(schema, table));
        } catch (ExecutionException e) {
            log.info("getTableMeta failed " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public void clearAll() {
        tableMetaCache.cleanUp();
    }
    private TableMeta getTableMeta0(String fullname) throws IOException {
        MysqlQueryExecutor executor = new MysqlQueryExecutor(connector);
        ResultSetPacket packet = executor.query("desc " + fullname);
        return new TableMeta(fullname, parserTableMeta(packet));
    }

    private List<FieldMeta> parserTableMeta(ResultSetPacket packet) {
        Map<String, Integer> nameMaps = new HashMap<String, Integer>(6, 1f);

        int index = 0;
        for (FieldPacket fieldPacket : packet.getFieldDescriptors()) {
            nameMaps.put(fieldPacket.getOriginalName(), index++);
        }

        int size = packet.getFieldDescriptors().size();
        int count = packet.getFieldValues().size() / packet.getFieldDescriptors().size();
        List<FieldMeta> result = new ArrayList<FieldMeta>();
        for (int i = 0; i < count; i++) {
            FieldMeta meta = new FieldMeta();
            // 做一个优化，使用String.intern()，共享String对象，减少内存使用
            meta.setColumnName(packet.getFieldValues().get(nameMaps.get(COLUMN_NAME) + i * size).intern());//you can read mysql packet protocol
            meta.setColumnType(packet.getFieldValues().get(nameMaps.get(COLUMN_TYPE) + i * size));
            meta.setIsNullable(packet.getFieldValues().get(nameMaps.get(IS_NULLABLE) + i * size));
            meta.setIskey(packet.getFieldValues().get(nameMaps.get(COLUMN_KEY) + i * size));
            meta.setDefaultValue(packet.getFieldValues().get(nameMaps.get(COLUMN_DEFAULT) + i * size));
            meta.setExtra(packet.getFieldValues().get(nameMaps.get(EXTRA) + i * size));

            result.add(meta);
        }

        return result;
    }

    private String getFullName(String schema, String table) {
        StringBuilder builder = new StringBuilder();
        return builder.append('`')
                .append(schema)
                .append('`')
                .append('.')
                .append('`')
                .append(table)
                .append('`')
                .toString();
    }

}
