package com.alibaba.datax.plugin.writer.nebulagraphwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum NebulaGraphWriterErrorCode implements ErrorCode {

    REQUIRED_VALUE("NebulaGraphWriter-00", "Parameter value is missing"),
    ILLEGAL_VALUE("NebulaGraphWriter-01", "Invalid parameter value"),
    RUNTIME_EXCEPTION("NebulaGraphWriter-02", "Runtime exception"),
    TYPE_ERROR("NebulaGraphWriter-03", "Data type mapping error");

    private final String code;
    private final String description;

    NebulaGraphWriterErrorCode(String code, String description) {
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
        return String.format("Error Code: [%s], Description: [%s]", this.code, this.description);
    }
}
