package com.alibaba.datax.plugin.reader.nebulagraphreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;

import com.vesoft.nebula.client.graph.data.DateWrapper;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.graph.exception.InvalidValueException;
import com.vesoft.nebula.jdbc.impl.NebulaDriver;

import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.List;

public class NebulaGraphReaderTest {

    static NebulaGraphReader.Task task;

    private static Connection conn;


    // 内部静态类也需要创建对象
    @BeforeClass
    public static void beforeClass() throws SQLException {
        task = new NebulaGraphReader.Task();
        NebulaDriver defaultDriver = new NebulaDriver();
        conn = DriverManager.getConnection("jdbc:nebula://cba", "root", "nebula");
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    public void taskInit() {
        // given
        Configuration conf = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"nebula\"," +
                "\"column\": [\"name\", \"age\"]," +
                "\"table\":[\"player\"]," +
                "\"querySql\": []," +
                "\"jdbcUrl\":\"jdbc:nebula://cba\"" +
                "}");
        task.setReaderSliceConfig(conf);
        String where = conf.getString("where");
        // 当配置文件中没有出现where字段时 其返回null
        // 而当出现该字段 用户未填写时，则返回空串
        if (where == null || StringUtils.isBlank(where)) {
            System.out.println("null or blank");
        }
        List<String> tables = conf.getList("table", String.class);
        List<String> columns = conf.getList("column", String.class);
        List<String> querySql = conf.getList("querySql", String.class);
        if (querySql == null || querySql.isEmpty()) {
            System.out.println("querySql is null or blank");
        }
    }

    @Test
    public void readQueryTagBynGql() throws SQLException {
        // given
        // test: 如果edgeType在配置文件中为空 是否有影响
        Configuration conf = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"nebula\"," +
                "\"column\": [\"name\", \"age\"]," +
                "\"table\":[\"follow\"]," +
                "\"edgeType\": [{\"srcTag\":\"player\",\"srcPrimaryKey\":\"srcPlayerName\"," +
                "\"dstTag\":\"player\",\"dstPrimaryKey\":\"dstPlayerName\"}]," +
                "\"jdbcUrl\":\"jdbc:nebula://cba\"" +
                "}");
        List<String> columns = conf.getList("column", String.class);
        task.setColumns(columns);
        String nGql = task.readQueryTagBynGql("player", false, 0);
        System.out.println("nGql: " + nGql);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(nGql);
        int cnt = 0;
        while (rs.next()) {
            Record rec = simulateBuildRecord(rs);
            System.out.println(rec);
            cnt++;
        }
        System.out.println("Count: " + cnt);
    }

    private Record simulateBuildRecord(ResultSet rs) {
        Record record = new DefaultRecord();
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
            throw new RuntimeException(e);
        }
        return record;
    }

    @Test
    public void jobInitCase1() {
        // given
        NebulaGraphReader.Job job = new NebulaGraphReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"nebula\"," +
                "\"column\": [\"name\", \"age\"]," +
                "\"where\": \"properties(vertex).age >= 22\"," +
                "\"connection\": [" +
                    "{" +
                        "\"table\":[\"player\"]," +
                        "\"jdbcUrl\":[\"jdbc:nebula://cba\"]" +
                    "}" +
                "]" +
                "}");
        job.setPluginJobConf(configuration);

        // when
        job.init();

        // then & assert
        Configuration conf = job.getPluginJobConf();

        Assert.assertEquals("root", conf.getString(Key.USERNAME));
        Assert.assertEquals("nebula", conf.getString(Key.PASSWORD));
        Assert.assertEquals("name", conf.getString("column[0]"));
        Assert.assertEquals("properties(vertex).age >= 22", conf.getString(Key.WHERE));
        Assert.assertEquals("player", conf.getString("connection[0].table[0]"));
        Assert.assertEquals("jdbc:nebula://cba", conf.getString("connection[0].jdbcUrl[0]"));
    }

    @Test
    public void jobInitCase2() {
        // given
        NebulaGraphReader.Job job = new NebulaGraphReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"nebula\"," +
                "\"connection\": [" +
                    "{" +
                        "\"querySql\":[\"LOOKUP ON player YIELD properties(vertex).name,properties(vertex).age\"]," +
                        "\"jdbcUrl\":[\"jdbc:nebula://cba\"]" +
                    "}" +
                "]" +
                "}");
        job.setPluginJobConf(configuration);

        // when
        job.init();

        // then & assert
        Configuration conf = job.getPluginJobConf();

        Assert.assertEquals("jdbc:nebula://cba", conf.getString("connection[0].jdbcUrl[0]"));
        Assert.assertEquals("LOOKUP ON player YIELD properties(vertex).name,properties(vertex).age", conf.getString("connection[0].querySql[0]"));
    }

    @Test
    public void jobSplitCase1() {
        // given
        NebulaGraphReader.Job job = new NebulaGraphReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"nebula\"," +
                "\"column\": [\"name\", \"age\"]," +
                "\"where\": \"properties(vertex).age >= 22\"," +
                "\"connection\": [" +
                "{" +
                "\"table\":[\"player\"]," +
                "\"jdbcUrl\":[\"jdbc:nebula://cba\"]" +
                "}" +
                "]" +
                "}");
        job.setPluginJobConf(configuration);

        // when
        job.init();
        List<Configuration> confList = job.split(1);

        // then & assert
        Assert.assertEquals(1, confList.size());

        Configuration conf = confList.get(0);
        Assert.assertEquals("root", conf.getString(Key.USERNAME));
        Assert.assertEquals("nebula", conf.getString(Key.PASSWORD));
        Assert.assertEquals("name", conf.getString("column[0]"));
        Assert.assertEquals("properties(vertex).age >= 22", conf.getString(Key.WHERE));
        Assert.assertEquals("player", conf.getString("table[0]"));
        Assert.assertEquals("jdbc:nebula://cba", conf.getString("jdbcUrl"));
    }

    @Test
    public void jobSplitCase2() {
        // given
        NebulaGraphReader.Job job = new NebulaGraphReader.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"nebula\"," +
                "\"connection\": [" +
                "{" +
                    "\"querySql\":[\"LOOKUP ON player YIELD properties(vertex).name,properties(vertex).age\"," +
                                  "\"LOOKUP ON player YIELD id(vertex)\"]," +
                    "\"jdbcUrl\":[\"jdbc:nebula://cba\",\"jdbc:nebula://nba\"]" +
                "}" +
                "]" +
                "}");
        job.setPluginJobConf(configuration);

        // when
        job.init();
        List<Configuration> confList = job.split(1);

        // then & assert
        Assert.assertEquals(2, confList.size());

        Configuration conf1 = confList.get(0);
        Assert.assertEquals("jdbc:nebula://cba", conf1.getString("jdbcUrl"));
        Assert.assertEquals("LOOKUP ON player YIELD properties(vertex).name,properties(vertex).age", conf1.getString("querySql[0]"));
        Configuration conf2 = confList.get(1);
        Assert.assertEquals("jdbc:nebula://nba", conf2.getString("jdbcUrl"));
        Assert.assertEquals("LOOKUP ON player YIELD properties(vertex).name,properties(vertex).age", conf2.getString("querySql[0]"));
        Assert.assertEquals("LOOKUP ON player YIELD id(vertex)", conf2.getString("querySql[1]"));
    }
}
