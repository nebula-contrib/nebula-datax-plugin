package com.alibaba.datax.plugin.reader.nebulagraphreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.vesoft.nebula.client.graph.data.DateWrapper;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.graph.exception.InvalidValueException;
import com.vesoft.nebula.jdbc.impl.NebulaDriver;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class NebulaGraphReader extends Reader {

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig;

        @Override
        public void init() {
            // 获取配置信息
            this.originalConfig = super.getPluginJobConf();

            // check username
            String username = this.originalConfig.getString(Key.USERNAME);
            if (StringUtils.isBlank(username)) {
                throw DataXException.asDataXException(NebulaGraphReaderErrorCode.REQUIRED_VALUE,
                        "Parameter [" + Key.USERNAME + "] is not set");
            }

            // check password
            String passwd = this.originalConfig.getString(Key.PASSWORD);
            if (StringUtils.isBlank(passwd)) {
                throw DataXException.asDataXException(NebulaGraphReaderErrorCode.REQUIRED_VALUE,
                        "Parameter [" + Key.PASSWORD + "] is not set");
            }

            // check connection
            List<Configuration> conns = this.originalConfig.getListConfiguration(Key.CONNECTION);
            if (conns == null || conns.isEmpty()) {
                throw DataXException.asDataXException(NebulaGraphReaderErrorCode.REQUIRED_VALUE,
                        "Parameter [" + Key.CONNECTION + "] is not set");
            }

            for (int i = 0; i < conns.size(); i++) {
                Configuration conn = conns.get(i);
                // check jdbcUrl
                List<Object> jdbcUrlList = conn.getList(Key.JDBC_URL);
                if (jdbcUrlList == null || jdbcUrlList.isEmpty()) {
                    throw DataXException.asDataXException(NebulaGraphReaderErrorCode.REQUIRED_VALUE,
                            "Parameter [" + Key.JDBC_URL + "] of connection [" + (i+1) + "] is not set");
                }
                // check table or querySql
                List<Object> querySqlList = conn.getList(Key.QUERY_SQL);
                if (querySqlList == null || querySqlList.isEmpty()) {
                    List<Object> table = conn.getList(Key.TABLE);
                    if (table == null || table.isEmpty()) {
                        throw DataXException.asDataXException(NebulaGraphReaderErrorCode.REQUIRED_VALUE,
                                "Parameter both [" + Key.TABLE + "] and [" + Key.QUERY_SQL + "] of connection[" + (i+1) + "] are note set");
                    }
                }
            }
        }

        @Override
        public void destroy() {

        }

        // 根据jdbcUrl的个数以及config的个数来决定job切分的个数
        // 一个Task对应一个jdbcUrl
        // TODO: 会出现querySql列表和jdbcUrl对应的图空间不一致 需要进行相应的异常处理或者过滤
        @Override
        public List<Configuration> split(int adviceNumber) {
            // ConfigList
            List<Configuration> readerSplitConfig = new ArrayList<>();
            List<Configuration> connList = this.originalConfig.getListConfiguration(Key.CONNECTION);
            // 遍历connection字段 获取jdbcUrl, table or querySql (均为json数组)
            for (Configuration conn : connList) {
                List<String> jdbcUrlList = conn.getList(Key.JDBC_URL, String.class);
                for (String jdbcUrl : jdbcUrlList) {
                    Configuration cloneConfig = this.originalConfig.clone();
                    cloneConfig.set(Key.JDBC_URL, jdbcUrl);
                    cloneConfig.set(Key.TABLE, conn.getList(Key.TABLE));
                    cloneConfig.set(Key.QUERY_SQL, conn.getList(Key.QUERY_SQL));
                    cloneConfig.remove(Key.CONNECTION);
                    readerSplitConfig.add(cloneConfig);
                }
            }
            return readerSplitConfig;
        }
    }

    public static class Task extends Reader.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private String mandatoryEncoding;
        private Connection conn;

        private List<String> tables;
        private List<String> columns;
        private List<String> querySql;

        private String where;
        private final int LOOKUP = 0, MATCH = 1, GO = 2, FETCH = 3;

        // 加载jdbc驱动类
        static {
            try {
                Class.forName("com.vesoft.nebula.jdbc.NebulaDriver");
            } catch (ClassNotFoundException ignored) {

            }
        }

        public void setColumns(List<String> columns) {this.columns = columns;}
        public void setReaderSliceConfig(Configuration conf) {this.readerSliceConfig = conf;}


        @Override
        public void init() {
            // 获取Task的Configuration
            this.readerSliceConfig = super.getPluginJobConf();

            String username = readerSliceConfig.getString(Key.USERNAME);
            String password = readerSliceConfig.getString(Key.PASSWORD);
            String jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);

            try {
                NebulaDriver defaultDriver = new NebulaDriver();
                this.conn = DriverManager.getConnection(jdbcUrl, username, password);
            } catch (Exception e) {
                throw DataXException.asDataXException(NebulaGraphReaderErrorCode.CONNECTION_FAILED,
                        "Parameter [" + Key.JDBC_URL + "] : " + jdbcUrl + "failed to connect due to: " + e.getMessage(), e);
            }
            this.tables = readerSliceConfig.getList(Key.TABLE, String.class);
            this.columns = readerSliceConfig.getList(Key.COLUMN, String.class);
            this.querySql = readerSliceConfig.getList(Key.QUERY_SQL, String.class);
            this.mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "UTF-8");
            this.where = readerSliceConfig.getString(Key.WHERE);
        }

        @Override
        public void destroy() {
            try {
                if (conn != null) conn.close();
            } catch (SQLException e) {
                LOG.error(e.getMessage(), e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            // 生成查询nGql集合
            List<String> nGqlList = new ArrayList<>();

            // 优先使用querySql
            if (querySql == null || querySql.isEmpty()) {
                // 遍历所有TAG
                for (String table : tables) {
                    if (where == null || StringUtils.isBlank(where)) {
                        nGqlList.add(readQueryTagBynGql(table, false, 0));
                    } else {
                        nGqlList.add(readQueryTagBynGql(table, true, 0));
                    }
                }
            } else {
                nGqlList.addAll(querySql);
            }

            // 遍历所有nGql集合
            for (String nGql : nGqlList) {
                try (Statement stmt = conn.createStatement()) {
                    LOG.debug(">>> " + nGql);
                    ResultSet rs = stmt.executeQuery(nGql);
                    while (rs.next()) {
                        Record record = buildRecord(recordSender, rs, mandatoryEncoding);
                        recordSender.sendToWriter(record);
                    }
                } catch (SQLException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }

        // 内置查询方法：Tag 默认采用LOOKUP语句
        // TODO
        public String readQueryTagBynGql(String tag, boolean isWhereSupported, int queryMode) {
            String whr = "";
            if (isWhereSupported) {
                whr = "WHERE " + where + " ";
            }
            switch (queryMode) {
                case LOOKUP: {
                    return queryTagByLookUpnGql(tag, whr);
                }
                case MATCH: {
                    // throw Exception
                    throw DataXException.asDataXException(NebulaGraphReaderErrorCode.UNSUPPORTED_QUERY_MODE, "Unsupported query mode match");
                }
                case GO: {

                }
                case FETCH: {

                }
                default:
                    return queryTagByLookUpnGql(tag, whr);
            }
        }

        // 仅支持column中出现标签属性 不支持id
        private String queryTagByLookUpnGql(String tag, String whr) {
            String s = "LOOKUP ON " + tag + whr + " YIELD " +
                    columns.stream().
                            map(column -> {
                                return "properties(vertex)." + column;})
                            .collect(Collectors.joining(","));
            return s.trim();
        }

        private Record buildRecord(RecordSender recordSender, ResultSet rs, String mandatoryEncoding) {
            Record record = recordSender.createRecord();
            try {
                // rs: 查询语句返回一行数据 索引从1开始
                ResultSetMetaData metaData = rs.getMetaData();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    ValueWrapper val = (ValueWrapper) rs.getObject(i);
                    if (val.isBoolean()) {
                        record.addColumn(new BoolColumn(val.asBoolean()));
                    } else if (val.isDouble()) {
                        record.addColumn(new DoubleColumn(val.asDouble()));
                    } else if (val.isString()) {
                        record.addColumn(new StringColumn(val.asString()));
                    } else if (val.isLong()) {
                        record.addColumn(new LongColumn(val.asLong()));
                    } else if (val.isDate()) {
                        DateWrapper dw = val.asDate();
                        record.addColumn(new DateColumn(new Date(dw.getYear(),dw.getMonth(),dw.getDay())));
                    } else if (val.isTime()) {
                        // val.asTime();
                    } else if (val.isDateTime()) {
                        // val.asDateTime();
                    } else if (val.isDuration()) {
                        // val.asDuration();
                    }
                }
            } catch (SQLException | UnsupportedEncodingException | InvalidValueException e) {
                throw DataXException.asDataXException(NebulaGraphReaderErrorCode.RUNTIME_EXCEPTION, e.getMessage(), e);
            }
            return record;
        }
    }
}
