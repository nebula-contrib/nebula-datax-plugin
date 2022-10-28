package com.alibaba.datax.plugin.writer.nebulagraphwriter;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.fastjson.JSONObject;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import com.vesoft.nebula.jdbc.impl.NebulaDriver;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DefaultDataHandlerTest {

    private static Connection conn;
    private static Session session;
    private static NebulaPool pool;

    private final TaskPluginCollector taskPluginCollector
            = new NebulaGraphWriter.Task().getTaskPluginCollector();

    @BeforeClass
    public static void beforeClass() throws SQLException, UnknownHostException, IOErrorException, AuthFailedException, ClientServerIncompatibleException, NotValidConnectionException {
        NebulaDriver defaultDriver = new NebulaDriver();
        conn = DriverManager.getConnection("jdbc:nebula://cba","root","nebula");
        // 利用nebula-java连接nebulaGraph
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setMaxConnSize(10);
        List<HostAddress> addresses = Arrays.asList(new HostAddress("127.0.0.1", 9669));
        pool = new NebulaPool();
        pool.init(addresses, nebulaPoolConfig);
        session = pool.getSession("root", "nebula", false);
        // 使用哪个图空间需要指定
        // 可以从jdbcUrl中截取 jdbc:nebula://cba
        session.execute("use cba");
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (conn != null) {
            conn.close();
        }
        if (session != null && pool != null) {
            session.release();
            pool.close();
        }
    }

    @Test
    public void writeBatchToTagBynGQL() throws Exception {
        // given
        // create Tag using nebula-java with execute DDL
        Configuration conf = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"nebula\"," +
                "\"column\": [\"name\", \"age\"]," +
                "\"table\":[\"player\"]," +
                "\"edgeType\": [{\"srcTag\":\"player\",\"srcPrimaryKey\":\"srcPlayerName\"," +
                                "\"dstTag\":\"player\",\"dstPrimaryKey\":\"dstPlayerName\"}]," +
                "\"jdbcUrl\":\"jdbc:nebula://cba\"," +
                "\"batchSize\": \"1000\"" +
                "}");
        List<Record> recordList = IntStream.range(11,21).mapToObj(i -> {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("member_" + i));
            record.addColumn(new LongColumn(18 + i));
            return record;
        }).collect(Collectors.toList());

        // when
        DefaultDataHandler handler = new DefaultDataHandler(conf, taskPluginCollector);
        List<String> tables = conf.getList("table", String.class);
        SchemaManager schemaManager = new SchemaManager(session);
        Map<String, TableMeta> tableMetas = schemaManager.loadTableMeta(tables);
        Map<String, List<ColumnMeta>> columnMetas = schemaManager.loadColumnMeta(tables, tableMetas);
        handler.setTableMetas(tableMetas);
        handler.setColumnMetas(columnMetas);
        handler.setSchemaManager(schemaManager);

        int count = handler.writeBatch(conn, recordList);

        // then & assert
        Assert.assertEquals(0, count);
    }

    @Test
    public void writeBatchToEdgeTypeBynGQL() throws Exception {
        // given
        // create EdgeType using nebula-java with execute DDL
        Configuration conf = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"nebula\"," +
                "\"column\": [\"srcPlayerName\", \"dstPlayerName\", \"degree\"]," +
                "\"table\":[\"follow\"]," +
                "\"edgeType\": [{\"srcTag\":\"player\",\"srcPrimaryKey\":\"srcPlayerName\"," +
                                "\"dstTag\":\"player\",\"dstPrimaryKey\":\"dstPlayerName\"}]," +
                "\"jdbcUrl\":\"jdbc:nebula://cba\"," +
                "\"batchSize\": \"1000\"" +
                "}");
        List<Record> recordList = IntStream.range(11,21).mapToObj(i -> {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("member_" + 1));
            record.addColumn(new StringColumn("member_" + i));
            record.addColumn(new LongColumn(100 + i));
            return record;
        }).collect(Collectors.toList());

        // when
        DefaultDataHandler handler = new DefaultDataHandler(conf, taskPluginCollector);
        List<String> tables = conf.getList("table", String.class);
        SchemaManager schemaManager = new SchemaManager(session);
        Map<String, TableMeta> tableMetas = schemaManager.loadTableMeta(tables);
        Map<String, List<ColumnMeta>> columnMetas = schemaManager.loadColumnMeta(tables, tableMetas);
        handler.setTableMetas(tableMetas);
        handler.setColumnMetas(columnMetas);
        handler.setSchemaManager(schemaManager);

        int count = handler.writeBatch(conn, recordList);

        // then & assert
        // 此处expected值和其他的sql insert语句不一样 无法返回affectedRows
        Assert.assertEquals(0, count);
    }

    @Test
    public void edgeTypeStatement() throws Exception {
        // when
        Configuration conf = Configuration.from("{" +
                "\"edgeType\": [{\"srcTag\":\"player\",\"srcPrimaryKey\":\"srcPlayerName\"," +
                                  "\"dstTag\":\"player\",\"dstPrimaryKey\":\"dstPlayerName\"}]" +
                "}");

        // assert
        Assert.assertEquals("player", conf.getString("edgeType[0].srcTag"));
        Assert.assertEquals("srcPlayerName", conf.getString("edgeType[0].srcPrimaryKey"));
        Assert.assertEquals("player", conf.getString("edgeType[0].dstTag"));
        Assert.assertEquals("dstPlayerName", conf.getString("edgeType[0].dstPrimaryKey"));
    }

    @Test
    public void tagStatement() throws Exception {
        // when
        Configuration conf = Configuration.from("{" +
                "\"tag\": [{\"primaryKey\":\"name\"}]" +
                "}");

        // assert
        Assert.assertEquals("name", conf.getString("tag[0].primaryKey"));
    }

    @Test
    public void jsonArrayObtainEdgeType() throws Exception {
        // when
        Configuration conf = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"nebula\"," +
                "\"column\": [\"srcName\", \"dstName\", \"degree\"]," +
                "\"table\":[\"follow\"]," +
                "\"edgeType\": [{\"srcTag\":\"player\",\"srcPrimaryKey\":\"srcPlayerName\"," +
                                "\"dstTag\":\"player\",\"dstPrimaryKey\":\"dstPlayerName\"}]," +
                "\"jdbcUrl\":\"jdbc:nebula://cba\"," +
                "\"batchSize\": \"1000\"" +
                "}");

        List<JSONObject> edgeTypes = conf.getList("edgeType", JSONObject.class);

        // then & assert
        Assert.assertEquals("player", edgeTypes.get(0).get("srcTag"));
        Assert.assertEquals("srcPlayerName", edgeTypes.get(0).get("srcPrimaryKey"));
        Assert.assertEquals("player", edgeTypes.get(0).get("dstTag"));
        Assert.assertEquals("dstPlayerName", edgeTypes.get(0).get("dstPrimaryKey"));
    }
}
