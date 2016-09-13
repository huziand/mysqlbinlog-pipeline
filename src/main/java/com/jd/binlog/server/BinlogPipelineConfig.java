package com.jd.binlog.server;

import com.jd.binlog.server.common.BinlogPipelineException;
import com.jd.binlog.server.common.ErrorCode;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by guoliming on 2015/6/24.
 */
public class BinlogPipelineConfig {

    private final Properties properties = new Properties();

    public BinlogPipelineConfig(String path) {
        try {
            FileInputStream fileInputStream = new FileInputStream(path);
            properties.load(fileInputStream);
        } catch (FileNotFoundException e) {
            throw new BinlogPipelineException(ErrorCode.FILE_NOT_FOUND, " file path : " + path);
        } catch (IOException e) {
            throw new BinlogPipelineException(ErrorCode.IO_EXCEPTION, " error reading file : " + path);
        }
    }

    public String getMasterMySQLAddress() {
        return properties.getProperty("mysql.address");
    }

    public int getMasterMySQLPort() {
        return Integer.valueOf(properties.getProperty("mysql.port"));
    }

    public String getMasterMySQLUser() {
        return properties.getProperty("mysql.user");
    }

    public String getMasterMySQLPassword() {
        return properties.getProperty("mysql.password");
    }

    public long getMasterMySQLSlaveId() {
        return Long.valueOf(properties.getProperty("mysql.slaveId"));
    }

    public long getBinlogStartPosition() {
        return Long.parseLong(properties.getProperty("binlog.start-position"));
    }

    public String getBinlogFileName() {
        return properties.getProperty("binlog.start-log-file");
    }

    public String getDatabase() {
        return properties.getProperty("mysql.database");
    }

    public String getTablePrefix() {
        return properties.getProperty("mysql.table-prefix");
    }

    public String getJProxySchema() {
        return properties.getProperty("jproxy.schema");
    }

    public String getJProxyTable() {
        return properties.getProperty("jproxy.table");
    }

    public String getJProxyAddress() {
        return properties.getProperty("jproxy.address");
    }

    public String getJProxyUser() {
        return properties.getProperty("jproxy.user");
    }

    public String getJProxyPassword() {
        return properties.getProperty("jproxy.password");
    }
}
