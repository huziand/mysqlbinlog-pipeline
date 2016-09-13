package com.jd.binlog.server.common;

/**
 * Created by guoliming on 2015/6/24.
 */
public enum ErrorCode {

    CONNECTION_FAILED(10001),
    PARSER_ERROR(10002),
    NOT_SUPPORT_EVENT(10003),
    FILE_NOT_FOUND(10004),
    IO_EXCEPTION(10005);

    private final int errorCode;

    ErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    @Override
    public String toString() {
        return errorCode + "-" + name();
    }
}
