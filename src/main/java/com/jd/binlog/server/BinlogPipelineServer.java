package com.jd.binlog.server;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.IOException;

/**
 * Created by guoliming on 2015/6/23.
 */
public class BinlogPipelineServer {

    public static final Logger log = Logger.getLogger(BinlogPipelineServer.class);

    public static void main(String[] args) {
        String configPath = System.getProperty("config") + File.separator + "config.properties";
        String log4j = System.getProperty("config") + File.separator + "log4j.properties";
        configPath = "D:\\workspace\\git\\binlog-pipeline\\conf\\config.properties";
        log4j = "D:\\workspace\\git\\binlog-pipeline\\conf\\log4j.properties";
        BinlogPipelineConfig config = new BinlogPipelineConfig(configPath);
        BinlogPipelineService service = new BinlogPipelineService(config);
        PropertyConfigurator.configure(log4j);
        try {
            service.start();
        } catch (IOException e) {
            log.info("Start BinlogPipelineServer failed");
            e.printStackTrace();
        }
    }
}
