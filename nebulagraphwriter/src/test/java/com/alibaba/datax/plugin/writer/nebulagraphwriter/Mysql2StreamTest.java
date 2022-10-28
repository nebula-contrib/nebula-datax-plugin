package com.alibaba.datax.plugin.writer.nebulagraphwriter;

import com.alibaba.datax.core.Engine;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class Mysql2StreamTest {

    @Test
    public void mysql2stream() throws Throwable {
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/mysql2stream.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }
}
