package com.alibaba.datax.plugin.reader.nebulagraphreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum NebulaGraphReaderErrorCode implements ErrorCode {

    REQUIRED_VALUE("NebulaGraphReader-00", "Parameter value is missing"),
    ILLEGAL_VALUE("NebulaGraphReader-01", "Invalid parameter value"),
    CONNECTION_FAILED("NebulaGraphReader-02", "Connection Error"),
    RUNTIME_EXCEPTION("NebulaGraphReader-03", "Runtime Exception"),
    UNSUPPORTED_QUERY_MODE("NebulaGraphReader-04", "Unsupported query mode");

    private final String code;
    private final String description;

    NebulaGraphReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Error Code: [%s], Description: [%s]. ", this.code, this.description);
    }
}
