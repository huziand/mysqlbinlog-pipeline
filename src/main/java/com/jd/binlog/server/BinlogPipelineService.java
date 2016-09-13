package com.jd.binlog.server;

import com.jd.binlog.connection.MysqlConnector;
import com.jd.binlog.connection.MysqlUpdateExecutor;
import com.jd.binlog.connection.packets.HeaderPacket;
import com.jd.binlog.connection.packets.client.BinlogDumpCommandPacket;
import com.jd.binlog.connection.utils.PacketManager;
import com.jd.binlog.dbsync.DirectLogFetcherChannel;
import com.jd.binlog.dbsync.LogContext;
import com.jd.binlog.dbsync.LogDecoder;
import com.jd.binlog.dbsync.LogEvent;
import com.jd.binlog.server.common.BinlogPipelineException;
import com.jd.binlog.server.common.ErrorCode;
import com.jd.binlog.server.common.TableMetaCache;
import com.jd.binlog.server.parser.LogEventConvert;
import org.apache.log4j.Logger;
import protocol.protobuf.CanalEntry;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class BinlogPipelineService {

    private Logger logger = Logger.getLogger(BinlogPipelineService.class);

    private MysqlConnector connector;
    private MysqlUpdateExecutor updateExecutor ;
    private MysqlConnector connectorTable;
    private TableMetaCache tableMetaCache;
    private JProxyConnector jProxyConnector;

    //log event convert
    private LogEventConvert eventParser;

    //configuration for tracker of mysql
    private final BinlogPipelineConfig config ;

    //entry position, manager the offset
    private BinlogPosition startPosition ;

    //multi Thread share queue
    private BlockingQueue<LogEvent> eventQueue = new LinkedBlockingQueue<LogEvent>();
    private AtomicBoolean running = new AtomicBoolean(true);

    public BinlogPipelineService(BinlogPipelineConfig config) {
        this.config = config;
        jProxyConnector = new JProxyConnector(config);
    }

    public void start()throws IOException{
        initMySQLConnection();
        prepareParserEvent();
        binlogDump();
        closeConnectionAfterDump();
    }

    private void initMySQLConnection() {
        connector = new MysqlConnector(config);
        connectorTable = new MysqlConnector(config);
        try {
            connector.connect();
            connectorTable.connect();
        } catch (IOException e){
            logger.error("connector MySQL failed : " + e.getMessage());
            running.set(false);
            throw new BinlogPipelineException(ErrorCode.CONNECTION_FAILED, e.getMessage());
        }
        updateExecutor = new MysqlUpdateExecutor(connector);
        logger.info("connect to mysql successful......");
    }

    private void closeConnectionAfterDump() throws IOException{
        connector.disconnect();
        connectorTable.disconnect();
        logger.info("close mysql connection successful......");
    }

    /**
     * prepare start position for binlog dump and log event parser
     */
    private void prepareParserEvent()throws IOException{
        startPosition = findStartPosition();
        if(startPosition == null) {
            running.set(false);
            throw new BinlogPipelineException(ErrorCode.CONNECTION_FAILED, "fetch start position failed");
        }
        logger.info("find binlog start-position successful......");
        tableMetaCache = new TableMetaCache(connectorTable);
        eventParser = new LogEventConvert();
        eventParser.setTableMetaCache(tableMetaCache);
    }

    /**
     * find start position include binlog file name and offset
     */
    private BinlogPosition findStartPosition()throws IOException{
        BinlogPosition entryPosition = findFileStartPosition();
        if(entryPosition == null){
            logger.info("find start position from local file failed");
            entryPosition = new BinlogPosition(config.getBinlogFileName(), config.getBinlogStartPosition());
            logger.info("find start position from config file successful");
        }
        return entryPosition;
    }

    /**
     * find position by file
     */
    private BinlogPosition findFileStartPosition() throws IOException{
        BinlogPosition entryPosition = new BinlogPosition();
        if(entryPosition.readBinlogPosFile()){
            if(!"".equals(entryPosition.getJournalName()) && entryPosition.getPosition() != 0){
                return entryPosition;
            }
            else {
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * dump the binlog data according to the start position
     */
    private void binlogDump() throws IOException{
        TakeDataThread takeData = new TakeDataThread();
        takeData.start();
        logger.info("start TakeDataThread successful...");

        PersistenceThread persisData = new PersistenceThread();
        persisData.start();
        logger.info("start PersistenceThread successful...");

        while(running.get()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }


    class PersistenceThread extends Thread{

        private Logger logger = Logger.getLogger(PersistenceThread.class);

        public void run(){
            logger.info("get the queue data to the local list");
            while(running.get() && !Thread.currentThread().isInterrupted()){
                final LogEvent event;
                try {
                    event = eventQueue.take();
                } catch (InterruptedException e){
                    logger.info("error taking log event from queue");
                    Thread.currentThread().interrupt();
                    return;
                }
                try {
                    CanalEntry.Entry entry = eventParser.parse(event);
                    ExecuteRowSqlService.parserAndExecute(entry, config, jProxyConnector);
                    startPosition.writeBinlogPosFile(event.getLogPos(), eventParser.getBinlogFileName());
                } catch (Exception e){
                    logger.error("parse log event failed!!!" + e.getMessage());
                    System.out.print("parse log event failed!!!" + e.getMessage());
                    continue;
                }
            }
        }
    }

    /**
     * Take the binlog data from the mysql
     */
    class TakeDataThread extends Thread{

        private LogEvent event;
        private DirectLogFetcherChannel fetcher;
        private LogDecoder decoder;
        private LogContext context;

        private Logger logger = Logger.getLogger(TakeDataThread.class);

        public void run()  {
            try {
                preRun();
                while (fetcher.fetch()) {
                    logger.info("fetch the binlog data (event) successfully...");
                    event = decoder.decode(fetcher, context);
                    if (event == null) {
                        logger.error("fetched event is null!!!");
                        throw new BinlogPipelineException(ErrorCode.PARSER_ERROR, "event is null!");
                    }
                    logger.info("TakeDataThread get event : " +
                            LogEvent.getTypeName(event.getHeader().getType()) +
                            ",----> now pos: " +
                            (event.getLogPos() - event.getEventLen()) +
                            ",----> next pos: " +
                            event.getLogPos() +
                            ",----> binlog file : " +
                            eventParser.getBinlogFileName());
                    System.out.println("TakeDataThread get event : " +
                            LogEvent.getTypeName(event.getHeader().getType()) +
                            ",----> now pos: " +
                            (event.getLogPos() - event.getEventLen()) +
                            ",----> next pos: " +
                            event.getLogPos() +
                            ",----> binlog file : " +
                            eventParser.getBinlogFileName());
                    try {
                        if(event != null) {
                            eventQueue.put(event);
                        }
                    } catch (InterruptedException e){
                        logger.error("eventQueue and entryQueue add data failed!!!");
                        throw new InterruptedIOException();
                    }
                }
            } catch (IOException e){
                logger.error("fetch data failed!!!");
                e.printStackTrace();
            }

        }

        public void preRun() throws IOException {
            logger.info("set the binlog configuration for the binlog dump");
            updateExecutor.update("set wait_timeout=9999999");
            updateExecutor.update("set net_write_timeout=1800");
            updateExecutor.update("set net_read_timeout=1800");
            updateExecutor.update("set names 'binary'");
            updateExecutor.update("set @master_binlog_checksum= '@@global.binlog_checksum'");
            updateExecutor.update("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");
            logger.info("send the binlog dump packet to mysql , let mysql set up a binlog dump thread in mysql");
            BinlogDumpCommandPacket binDmpPacket = new BinlogDumpCommandPacket();
            binDmpPacket.binlogFileName = startPosition.getJournalName();
            binDmpPacket.binlogPosition = startPosition.getPosition();
            binDmpPacket.slaveServerId = config.getMasterMySQLSlaveId();
            byte[] dmpBody = binDmpPacket.toBytes();
            HeaderPacket dmpHeader = new HeaderPacket();
            dmpHeader.setPacketBodyLength(dmpBody.length);
            dmpHeader.setPacketSequenceNumber((byte) 0x00);
            PacketManager.write(connector.getChannel(), new ByteBuffer[]{ByteBuffer.wrap(dmpHeader.toBytes()), ByteBuffer.wrap(dmpBody)});
            logger.info("initialize the MySQL sync class");
            fetcher = new DirectLogFetcherChannel(connector.getReceiveBufferSize());
            fetcher.start(connector.getChannel());
            decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            context = new LogContext();
        }
    }

    public static String getStringFromByteArray(byte[] bytes){
        String inString = String.valueOf(bytes[0]);
        for(int i=1;i<=bytes.length-1;i++){
            inString+=","+String.valueOf(bytes[i]);
        }
        return(inString);
    }

    public static byte[] getByteArrayFromString(String inString){
        String[] stringBytes = inString.split(",");
        byte[] bytes = new byte[stringBytes.length];
        for(int i=0;i<=stringBytes.length-1;i++){
            bytes[i] = Integer.valueOf(stringBytes[i]).byteValue();
        }
        return(bytes);
    }
}


