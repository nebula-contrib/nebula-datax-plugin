package com.alibaba.datax.plugin.writer.nebulagraphwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.InvalidValueException;
import com.vesoft.nebula.client.graph.net.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);

    private final Session session;

    public SchemaManager(Session session) {
        this.session = session;
    }

    // 加载表元信息
    public Map<String, TableMeta> loadTableMeta(List<String> tables) throws DataXException {
        Map<String, TableMeta> tableMetas = new HashMap<>();
        // 通过show系统级别nGql查询表元信息
        try {
            // 加载Tag类型表的元信息
            ResultSet rs = session.execute("show tags");
            for (ValueWrapper colVal : rs.colValues("Name")) {
                TableMeta tableMeta = buildTagTableMeta(colVal);
                if (!tables.contains(tableMeta.name)) continue;
                tableMetas.put(tableMeta.name, tableMeta);
            }
            // 加载EdgeType类型表的元信息
            rs = session.execute("show edges");
            for (ValueWrapper colVal : rs.colValues("Name")) {
                TableMeta tableMeta = buildEdgeTypeTableMeta(colVal);
                if (!tables.contains(tableMeta.name)) continue;
                tableMetas.put(tableMeta.name, tableMeta);
            }
            // 防止用户配置文件中出现不存在的表
            for (String tbname : tables) {
                if (!tableMetas.containsKey(tbname)) {
                    throw DataXException.asDataXException(NebulaGraphWriterErrorCode.RUNTIME_EXCEPTION,
                            "MetaData of table " + tbname + "is empty!");
                }
            }
        } catch (IOErrorException | UnsupportedEncodingException e) {
            throw DataXException.asDataXException(NebulaGraphWriterErrorCode.RUNTIME_EXCEPTION, e.getMessage());
        }
        return tableMetas;
    }

    public Map<String, List<ColumnMeta>> loadColumnMeta(List<String> tables, Map<String, TableMeta> tableMetas) throws DataXException {
        Map<String, List<ColumnMeta>> columnMetas = new HashMap<>();
        // 通过describe系统级别nGql查询字段元信息
        for (String table : tables) {
            List<ColumnMeta> colMetaList = new ArrayList<>();
            ResultSet rs;
            try {
                if (tableMetas.containsKey(table) &&
                        tableMetas.get(table).tableType == TableType.TAG) {
                    rs = session.execute("describe tag " + table);
                    for (int i = 0; i < rs.colValues("Field").size(); i++) {
                        ColumnMeta columnMeta = buildColumnMeta(rs, i);
                        colMetaList.add(columnMeta);
                    }
                }
                else if (tableMetas.containsKey(table) &&
                        tableMetas.get(table).tableType == TableType.EDGE_TYPE) {
                    rs = session.execute("describe edge " + table);
                    for (int i = 0; i < rs.colValues("Field").size(); i++) {
                        ColumnMeta columnMeta = buildColumnMeta(rs, i);
                        colMetaList.add(columnMeta);
                    }
                }
                else {
                    // throw DataXException: 不存在当前table
                    throw DataXException.asDataXException(NebulaGraphWriterErrorCode.ILLEGAL_VALUE,
                            "GraphSpace doesn't exist table " + table);
                }
            } catch (IOErrorException | UnsupportedEncodingException e) {
                throw DataXException.asDataXException(NebulaGraphWriterErrorCode.RUNTIME_EXCEPTION, e.getMessage());
            }

            if (colMetaList.isEmpty()) {
                LOG.error("Column metadata of " + table + " is empty!");
                continue;
            }

            LOG.debug("Load column metadata of " + table + ": " +
                    colMetaList.stream().map(ColumnMeta::toString).collect(Collectors.joining(",", "[", "]"))
            );
            columnMetas.put(table, colMetaList);
        }
        return columnMetas;
    }

    // 创建Tag类型表的元信息
    private TableMeta buildTagTableMeta(ValueWrapper colVal) throws InvalidValueException, UnsupportedEncodingException {
        TableMeta tableMeta = new TableMeta();
        String name = colVal.asString();
        tableMeta.tableType = TableType.TAG;
        tableMeta.name = name;

        if (LOG.isDebugEnabled()){
            LOG.debug("Load tag metadata of " + tableMeta.name + ": " + tableMeta);
        }
        return tableMeta;
    }

    // 创建EdgeType类型表的元信息
    private TableMeta buildEdgeTypeTableMeta(ValueWrapper colVal) throws InvalidValueException, UnsupportedEncodingException {
        TableMeta tableMeta = new TableMeta();
        String name = colVal.asString();
        tableMeta.tableType = TableType.EDGE_TYPE;
        tableMeta.name = name;

        if (LOG.isDebugEnabled()){
            LOG.debug("Load edgeType metadata of " + tableMeta.name + ": " + tableMeta);
        }
        return tableMeta;
    }

    // 创建column字段元信息(Tag和EdgeType类型的字段元信息格式相同)
    private ColumnMeta buildColumnMeta(ResultSet rs, int idx) throws InvalidValueException, UnsupportedEncodingException {
        ColumnMeta columnMeta = new ColumnMeta();
        columnMeta.field = rs.colValues("Field").get(idx).asString();
        columnMeta.type = rs.colValues("Type").get(idx).asString();
        columnMeta.Null = rs.colValues("Null").get(idx).asString();
        return columnMeta;
    }
}
