package com.alibaba.datax.plugin.writer.nebulagraphwriter;

import com.alibaba.datax.core.Engine;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class Mysql2NebulaGraphTest {

    @Test
    public void mysql2nebulaGraph() throws Throwable {
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/mysql2nebula.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }
}
