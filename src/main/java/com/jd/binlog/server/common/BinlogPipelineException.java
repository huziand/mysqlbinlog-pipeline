package com.jd.binlog.server.common;

/**
 * Created by guoliming on 2015/6/24.
 */
public class BinlogPipelineException extends RuntimeException{

    private final ErrorCode errorCode;
    public BinlogPipelineException(ErrorCode errorCode, String message) {
        this(errorCode, message, null);
    }

    public BinlogPipelineException(ErrorCode errorCode, Throwable throwable) {
        this(errorCode, null, throwable);
    }

    public BinlogPipelineException(ErrorCode errorCode, String message, Throwable throwable) {
        super(message, throwable);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        String message = getErrorCode().toString();
        if (super.getMessage() != null) {
            message += " | " + getCause().getMessage();
        }
        return message;
    }
}
