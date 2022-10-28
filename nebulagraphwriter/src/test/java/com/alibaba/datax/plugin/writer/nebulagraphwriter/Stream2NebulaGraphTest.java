package com.alibaba.datax.plugin.writer.nebulagraphwriter;

import com.alibaba.datax.core.Engine;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class Stream2NebulaGraphTest {

    @Test
    public void s2n_case() throws Throwable {
        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/defaultJob.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }
}
