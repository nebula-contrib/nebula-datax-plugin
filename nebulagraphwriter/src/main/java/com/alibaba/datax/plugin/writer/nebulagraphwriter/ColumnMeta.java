package com.alibaba.datax.plugin.writer.nebulagraphwriter;

public class ColumnMeta {
    String field;
    String type;
    String Null;
    String Default;
    String Comment;
    Object value;

    @Override
    public String toString() {
        return "ColumnMeta{" +
                "field='" + field + '\'' +
                ", type='" + type + '\'' +
                ", Null='" + Null + '\'' +
                ", Default='" + Default + '\'' +
                ", Comment='" + Comment + '\'' +
                ", value='" + value +
                '}';
    }
}
