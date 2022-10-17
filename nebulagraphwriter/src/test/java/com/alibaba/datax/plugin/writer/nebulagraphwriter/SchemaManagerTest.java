package com.alibaba.datax.plugin.writer.nebulagraphwriter;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SchemaManagerTest {

    private static Session session;
    private static NebulaPool pool;

    @BeforeClass
    public static void beforeClass() throws UnknownHostException, IOErrorException, AuthFailedException, ClientServerIncompatibleException, NotValidConnectionException {
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
    public static void afterClass() {
        if (session != null && pool != null) {
            session.release();
            pool.close();
        }
    }

    @Test
    public void loadTableMeta() throws IOErrorException, UnsupportedEncodingException {
        // given
        SchemaManager schemaManager = new SchemaManager(session);
        List<String> tables = Arrays.asList("player", "follow");

        // when
        Map<String, TableMeta> tableMetaMap = schemaManager.loadTableMeta(tables);

        // then & assert
        TableMeta player = tableMetaMap.get("player");
        Assert.assertEquals(TableType.TAG, player.tableType);
        Assert.assertEquals("player", player.name);

        TableMeta follow = tableMetaMap.get("follow");
        Assert.assertEquals(TableType.EDGE_TYPE, follow.tableType);
        Assert.assertEquals("follow", follow.name);
    }

    @Test
    public void loadColumnMetas() {
        // given
        SchemaManager schemaManager = new SchemaManager(session);
        List<String> tables = Arrays.asList("player", "follow");
        Map<String, TableMeta> tableMetaMap = schemaManager.loadTableMeta(tables);

        // when
        Map<String, List<ColumnMeta>> columnMetaMap = schemaManager.loadColumnMeta(tables, tableMetaMap);

        // then & assert
        List<ColumnMeta> player = columnMetaMap.get("player");
        Assert.assertEquals(2, player.size());
    }
}
