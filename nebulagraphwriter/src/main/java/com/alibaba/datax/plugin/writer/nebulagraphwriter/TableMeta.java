package com.alibaba.datax.plugin.writer.nebulagraphwriter;

public class TableMeta {
    TableType tableType;
    String name;

    @Override
    public String toString() {
        return "TableMeta{" +
                "tableType" + tableType +
                ", name='"  + name + '\'' +
                '}';
    }
}
