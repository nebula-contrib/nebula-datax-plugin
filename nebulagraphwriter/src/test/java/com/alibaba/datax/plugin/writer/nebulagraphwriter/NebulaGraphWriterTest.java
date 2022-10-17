package com.alibaba.datax.plugin.writer.nebulagraphwriter;

import com.alibaba.datax.common.util.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class NebulaGraphWriterTest {

    NebulaGraphWriter.Job job;

    @Before
    public void before() {
        job = new NebulaGraphWriter.Job();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"nebula\"," +
                "\"column\": [\"name\", \"age\"]," +
                "\"connection\": [" +
                                    "{" +
                                        "\"table\":[\"player\"]," +
                                        "\"edgeType\": [{\"srcTag\":\"player\",\"srcPrimaryKey\":\"srcPlayerName\"," +
                                                        "\"dstTag\":\"player\",\"dstPrimaryKey\":\"dstPlayerName\"}]," +
                                        "\"jdbcUrl\":\"jdbc:nebula://cba\"" +
                                    "}" +
                                "]," +
                "\"batchSize\": \"1000\"" +
                "}");
        job.setPluginJobConf(configuration);
    }

    @Test
    public void jobInit() {
        // when
        job.init();

        // assert
        Configuration conf = job.getPluginJobConf();

        Assert.assertEquals("root", conf.getString("username"));
        Assert.assertEquals("nebula", conf.getString("password"));
        Assert.assertEquals("jdbc:nebula://cba", conf.getString("connection[0].jdbcUrl"));
        Assert.assertEquals(new Integer(1000), conf.getInt("batchSize"));
        Assert.assertEquals("name", conf.getString("column[0]"));
        Assert.assertEquals("age", conf.getString("column[1]"));
    }

    @Test
    public void jobSplit() {
        // when
        job.init();
        List<Configuration> confList = job.split(10);

        // assert
        Assert.assertEquals(10, confList.size());
        for (Configuration conf : confList) {
            Assert.assertEquals("root", conf.getString("username"));
            Assert.assertEquals("nebula", conf.getString("password"));
            Assert.assertEquals("jdbc:nebula://cba", conf.getString("jdbcUrl"));
            Assert.assertEquals(new Integer(1000), conf.getInt("batchSize"));
            Assert.assertEquals("name", conf.getString("column[0]"));
            Assert.assertEquals("age", conf.getString("column[1]"));
        }
    }

}
