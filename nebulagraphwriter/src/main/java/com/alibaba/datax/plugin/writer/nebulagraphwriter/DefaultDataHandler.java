package com.alibaba.datax.plugin.writer.nebulagraphwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import com.vesoft.nebula.jdbc.impl.NebulaDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class DefaultDataHandler implements DataHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDataHandler.class);

    private final TaskPluginCollector taskPluginCollector;
    private final String username;
    private final String password;
    private final String jdbcUrl;
    private final int batchSize;

    private final List<String> tables;
    private final List<String> columns;
    private final List<JSONObject> edgeTypes;

    private Map<String, TableMeta> tableMetas;
    private Map<String, List<ColumnMeta>> columnMetas;

    private SchemaManager schemaManager;

    // Setters for List and Schema
    public void setTableMetas(Map<String, TableMeta> tableMetas) {
        this.tableMetas = tableMetas;
    }

    public void setColumnMetas(Map<String, List<ColumnMeta>> columnMetas) {
        this.columnMetas = columnMetas;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    // 数据库驱动加载常用方法 利用静态方法加载数据库驱动类
    static {
        try {
            Class.forName("com.vesoft.nebula.jdbc.NebulaDriver");
        } catch (ClassNotFoundException ignored) {

        }
    }

    public DefaultDataHandler(Configuration conf, TaskPluginCollector taskPluginCollector) {
        this.username = conf.getString(Key.USERNAME);
        this.password = conf.getString(Key.PASSWORD);
        this.jdbcUrl = conf.getString(Key.JDBC_URL);
        this.batchSize = conf.getInt(Key.BATCH_SIZE, Constants.DEFAULT_BATCH_SIZE);
        this.tables = conf.getList(Key.TABLE, String.class);
        this.columns = conf.getList(Key.COLUMN, String.class);
        this.edgeTypes = conf.getList(Key.EDGE_TYPE, JSONObject.class);
        this.taskPluginCollector = taskPluginCollector;
    }

    @Override
    public int handle(RecordReceiver rec, TaskPluginCollector collector) {
        int count = 0, affectedRows = 0;

        try {
            NebulaDriver defaultDriver = new NebulaDriver();
            Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
            Session session = getSession();
            LOG.info("Connection[ jdbcUrl: " + jdbcUrl + ", username: " + username
                    + "] established.");
            // 初始化SchemaManager 并且进行元信息的加载
            if (schemaManager == null) {
                this.schemaManager = new SchemaManager(session);
                // 根据Configuration 也就是配置文件中的tables 表名列表 加载 tableMeta
                this.tableMetas = schemaManager.loadTableMeta(tables);
                // 根据Configuration 配置文件中tables 表名列表 加载 columnMeta列表(一个表对应一个字段列表)
                this.columnMetas = schemaManager.loadColumnMeta(tables, tableMetas);
            }

            // writer端Record缓存队列
            List<Record> recordBuffer = new ArrayList();
            Record record;
            for (int i = 1; (record = rec.getFromReader()) != null; i++) {
                if (i % batchSize != 0) {
                    recordBuffer.add(record); // 逐渐将接受到的record加入到缓冲中
                } else {
                    try {
                        // i == batchSize倍数时 还差一个record未加入到缓冲队列中
                        recordBuffer.add(record);
                        affectedRows += writeBatch(conn, recordBuffer);
                    } catch (Exception e) {
                        // 脏数据处理
                        LOG.warn("Insert one row of record, due to error: " + e.getMessage());
                        affectedRows += writeEachRow(conn, recordBuffer);
                    }
                    recordBuffer.clear();
                }
                count++;
            }
            // 如果缓冲队列仍有record 即当最后一部分 也就是从发送端接收完毕 但是未满足batchSize的record集合
            if (!recordBuffer.isEmpty()) {
                try {
                    affectedRows += writeBatch(conn, recordBuffer);
                } catch (Exception e) {
                    // 脏数据处理
                    LOG.warn("Insert one row of record, due to error: " + e.getMessage());
                    affectedRows += writeEachRow(conn, recordBuffer);
                }
                recordBuffer.clear();
            }

        } catch (Exception e) {
            throw DataXException.asDataXException(NebulaGraphWriterErrorCode.RUNTIME_EXCEPTION, e.getMessage());
        }

        if (affectedRows != count) {
            LOG.error("record missing");
        }

        return affectedRows;
    }

    // 当record出现脏数据时的写入处理逻辑
    private int writeEachRow(Connection conn, List<Record> recordBuffer) {
        int affectedRows = 0;
        for (Record record : recordBuffer) {
            List<Record> recordList = new ArrayList<>();
            recordList.add(record);
            try {
                affectedRows += writeBatch(conn, recordList);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                this.taskPluginCollector.collectDirtyRecord(record, e);
            }
        }
        return affectedRows;
    }

    // write写入方法的逻辑实现
    public int writeBatch(Connection conn, List<Record> recordBuffer) throws Exception {
        int affectedRows = 0;
        // 遍历配置文件中待同步的table 根据表类型分别对应写入nGQL语句
        int tagNo = 0, edgeNo = 0;
        for (String table : tables) {
            TableMeta tableMeta = tableMetas.get(table);
            switch (tableMeta.tableType) {
                case TAG:
                    // 参数增加tagNo
                    affectedRows += writeBatchToTagBynGQL(conn, table, recordBuffer);
                    tagNo++;
                    break;
                case EDGE_TYPE:
                    affectedRows += writeBatchToEdgeTypeBynGQL(conn, table, recordBuffer, edgeNo);
                    edgeNo++;
                    break;
                default:
            }
        }
        // 返回写入成功的行数
        return affectedRows;
    }

    // TAG的写入nGQL的具体实现
    public int writeBatchToTagBynGQL(Connection conn, String table, List<Record> recordBuffer) throws Exception {
        List<ColumnMeta> colMetas = this.columnMetas.get(table);
        // 利用StringBuilder拼接insert语句
        StringBuilder sb = new StringBuilder();
        // 拼接sb生成insert nGql语句 (字段部分)
        sb.append("INSERT VERTEX ").append(table).append(" ")
                .append(colMetas.stream().filter(colMeta -> columns.contains(colMeta.field))
                        .map(colMeta -> {return colMeta.field;})
                        .collect(Collectors.joining(",","(",")")))
                        .append(" VALUES ");

        // 拼接values部分
        // insert vertex player(name, age) values "player100":("Lim Kee", 23)
        // VID由于要求是整个图空间中唯一 并且其并非是节点的标签 其属于独立于标签属性的唯一标识属性
        // 因此我们采取table+主键的形式生成VID 如有需求则需要在配置文件中指定VID的长度 也就是FIXED_STRING(N)
        // 中N的大小
        int row = 0;
        for (Record record : recordBuffer) {
            row++;
            for (int i = 0; i < colMetas.size(); i++) {
                ColumnMeta colMeta = colMetas.get(i);
                if (!columns.contains(colMeta.field)) continue;
                String colVal = buildColumnValue(colMeta, record);
                if (i == 0) {
                    String vid = table + "_" + colVal.substring(1,colVal.length()-1); // 生成VID

                    sb.append("\"").append(vid).append("\"").append(":");
                    sb.append("(").append(colVal);
                }
                else sb.append(",").append(colVal);
            }
            if (row == recordBuffer.size()) sb.append(")");
            else sb.append("),");
        }
        String nGql = sb.toString();
        return executeUpdate(conn, nGql);
    }

    // EDGE_TYPE的写入nGQL的具体实现
    public int writeBatchToEdgeTypeBynGQL(Connection conn, String table, List<Record> recordBuffer, int edgeNo) throws Exception {
        List<ColumnMeta> colMetas = this.columnMetas.get(table);
        JSONObject edgeType = this.edgeTypes.get(edgeNo);
        // 利用StringBuilder拼接insert语句
        StringBuilder sb = new StringBuilder();
        // 起始id 终点id 以及rank 查一下nebula中如何写的这部分insert语句 源数据库端需要遵守约定
        // 拼接sb生成insert nGql语句 (字段部分)
        sb.append("INSERT EDGE ").append(table).append(" ")
                .append(colMetas.stream().filter(colMeta -> columns.contains(colMeta.field))
                        .map(colMeta -> {return colMeta.field;})
                        .collect(Collectors.joining(",","(",")")))
                .append(" VALUES ");

        // values 部分
        // insert边语句格式:
        // insert edge follow(degree) values "player100"->"player101"@rank:(val)
        // 规定: 第一列和第二列字段是src_id和dst_id 起点主键和终点主键
        // 采用的insert批量插入
        // TODO: 此版本暂时不支持rank 后续待开发
        int row = 0;

        // 构造srcVid/dstVid ColumnMeta
        ColumnMeta srcVidCol = new ColumnMeta();
        srcVidCol.field = edgeType.get("srcPrimaryKey").toString();
        ColumnMeta dstVidCol = new ColumnMeta();
        dstVidCol.field = edgeType.get("dstPrimaryKey").toString();

        for (Record record : recordBuffer) {
            row++;
            String srcVidKey = buildColumnValue(srcVidCol, record);
            String dstVidKey = buildColumnValue(dstVidCol, record);
            // 生成dstVid和srcVid
            String srcVid = edgeType.get("srcTag") + "_" + srcVidKey.substring(1,srcVidKey.length()-1);
            String dstVid = edgeType.get("dstTag") + "_" + dstVidKey.substring(1,dstVidKey.length()-1);
            sb.append("\"").append(srcVid).append("\"");
            sb.append("->").append("\"").append(dstVid).append("\"");
            sb.append(":");
            sb.append("(");
            for (int i = 0; i < colMetas.size(); i++) {
                ColumnMeta colMeta = colMetas.get(i);
                if (!columns.contains(colMeta.field)) continue;
                String colVal = buildColumnValue(colMeta, record);
                if (i == 0) sb.append(colVal);
                else sb.append(",").append(colVal);
            }
            if (row == recordBuffer.size()) sb.append(")");
            else sb.append("),");
        }

        String nGql = sb.toString();
        return executeUpdate(conn, nGql);
    }

    // 创建字段值 根据record中的值进行匹配
    private String buildColumnValue(ColumnMeta colMeta, Record record) throws Exception {
        Column column = record.getColumn(indexOf(colMeta.field));
        Column.Type type = column.getType();
        switch (type) {
            // 待定
            case DATE:
                String val = column.asString();
                if (colMeta.type.equals("date")) {
                    return "date(" + "\"" + val + "\"" + ")";
                } else if (colMeta.type.equals("time")) {
                    return "time(" + "\"" + val + "\"" + ")";
                } else if (colMeta.type.equals("datetime")) {
                    return "datetime(" + "\"" + val + "\"" + ")";
                } else {
                    return "\"" + val + "\"";
                }
            case BYTES:
            case STRING:
                String value = column.asString();
                return "\"" + value + "\"";
            case NULL:
            case BAD:
                return "NULL";
            case BOOL:
            case DOUBLE:
            case INT:
            case LONG:
                return column.asString();
            default:
                throw new Exception("Invalid column type: " + type);
        }
    }

    // insert语句的包装方法
    private int executeUpdate(Connection conn, String nGql) throws SQLException {
        int cnt;
        try (Statement stmt = conn.createStatement()) {
            LOG.debug(">>>" + nGql);
            cnt = stmt.executeUpdate(nGql);
        }
        // TODO: cnt
        return cnt;
    }

    private Session getSession() throws UnknownHostException, IOErrorException, AuthFailedException, ClientServerIncompatibleException, NotValidConnectionException, UnsupportedEncodingException {
        // 利用nebula-java连接nebulaGraph
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setMaxConnSize(10);
        List<HostAddress> addresses = Arrays.asList(new HostAddress("127.0.0.1", 9669));
        NebulaPool pool = new NebulaPool();
        pool.init(addresses, nebulaPoolConfig);
        Session session = pool.getSession("root", "nebula", false);
        // 使用哪个图空间需要指定
        // 可以从jdbcUrl中截取 jdbc:nebula://cba
        session.execute("use cba"); // throws声明了Exception是否还需要try-catch处理
        return session;
    }

    private int indexOf(String colName) throws DataXException {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(colName))
                return i;
        }
        throw DataXException.asDataXException(NebulaGraphWriterErrorCode.RUNTIME_EXCEPTION,
                "Cannot find col: " + colName + " in columns: " + columns);
    }
}
