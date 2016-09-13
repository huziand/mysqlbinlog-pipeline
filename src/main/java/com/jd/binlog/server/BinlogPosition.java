package com.jd.binlog.server;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by hp on 14-9-2.
 */
public class BinlogPosition {

    private static final Logger log = Logger.getLogger(BinlogPosition.class);
    //当前位点offset 所在的 binlog 文件名
    private String journalName ;

    //当前binlog文件的 具体位点的offset量
    private Long position;

    //位点信息的存储，文件位置
    private static String binlogPosFileName;

    static {
        binlogPosFileName = System.getProperty("config") + File.separator + "BinlogPosition";
        binlogPosFileName = "D:\\workspace\\git\\binlog-pipeline\\logs\\BinlogPosition";
    }
    public BinlogPosition() {
        journalName = null;
        position = null;
    }

    public BinlogPosition(String journalName, Long position) {
        this.journalName = journalName;
        this.position = position;
    }

    public String getJournalName() {
        return journalName;
    }

    public Long getPosition() {
        return position;
    }

    public void setPosition(Long position) {
        this.position = position;
    }


    //位点信息IO
    //读取位点信息，并加载在内存中
    public boolean readBinlogPosFile() throws IOException {
        File datFile = new File(this.binlogPosFileName);
        if(!datFile.exists()||datFile.isDirectory()){
            return false;
        }
        BufferedReader br = new BufferedReader(new FileReader(datFile));
        String dataString = br.readLine();
        String [] datSplit = dataString.split(":");
        this.journalName = datSplit[0];
        this.position = Long.valueOf(datSplit[1]);
        log.info("read binlog position from local file, position = " + position + ", binlog file = " + journalName);
        br.close();
        return true;
    }

    //从内存中的位点信息 写到磁盘
    public void writeBinlogPosFile(long position, String binlogFileName) throws IOException{
        File datFile = new File(this.binlogPosFileName);
        if (!datFile.exists()) {
            datFile.createNewFile();
        }
        BufferedWriter bw = new BufferedWriter(new FileWriter(datFile));
        String dataString = binlogFileName + ":" + position;
        bw.write(dataString);
        bw.newLine();
        bw.flush();
        bw.close();
        log.info("write binlog position to local file success");
    }
}
