package com.alibaba.datax.plugin.reader.nebulagraphreader;

import com.alibaba.datax.core.Engine;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class NebulaGraph2MysqlTest {

    @Test
    public void nebula2mysql() throws Throwable {
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/n2mysql.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }
}
