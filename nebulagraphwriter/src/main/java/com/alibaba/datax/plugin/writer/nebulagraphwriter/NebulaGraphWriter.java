package com.alibaba.datax.plugin.writer.nebulagraphwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class NebulaGraphWriter extends Writer {
    /*
    此类为nebulagraphwriter插件的入口类
    按照DataX框架的模版，需要实现两个内部静态类Job和Task
    两部分最核心的方法分别为split和startWrite
     */
    private static final String PEER_PLUGIN_NAME = "peerPluginName"; // reader端插件名称

    public static class Job extends Writer.Job {
        private Configuration originalConfig;
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        @Override
        public void init() {
            // 获取当前用户的json配置 并且对各部分进行赋值
            this.originalConfig = super.getPluginJobConf();
            // 获取reader端插件名称
            this.originalConfig.set(PEER_PLUGIN_NAME, getPeerPluginName());
            // 获取各种参数并进行基础的验证

            // check username

            // check password

            // check connection

            // check column (tdengine 未实现)
        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            // 此方法为整个Job中最核心的方法
            // 其返回对Task的配置列表(复制Job的配置+进行的自定义处理)
            List<Configuration> writerSplitConfig = new ArrayList<>();

            // Key中定义的字符串常量
            List<Object> conns = this.originalConfig.getList(Key.CONNECTION);
            for (int i = 0; i < mandatoryNumber; i++) {
                // 对于Task配置Configuration对象 直接拷贝Job中的配置
                Configuration cloneConfig = this.originalConfig.clone();
                Configuration conf = Configuration.from(conns.get(0).toString());
                // 此步骤主要是为了去掉json中table和jdbc外部的connection字段
                // 便于后续可以直接通过Key来访问json字符串
                cloneConfig.set(Key.JDBC_URL, conf.getString(Key.JDBC_URL));
                cloneConfig.set(Key.TABLE, conf.getList(Key.TABLE));
                cloneConfig.set(Key.EDGE_TYPE, conf.getList(Key.EDGE_TYPE));
                cloneConfig.remove(Key.CONNECTION);
                writerSplitConfig.add(cloneConfig);
            }
            return writerSplitConfig;
        }
    }

    public static class Task extends Writer.Task {

        private Configuration writerSliceConfig; // 当前Task的配置信息
        private TaskPluginCollector taskPluginCollector; // 脏数据处理
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();
            this.taskPluginCollector = super.getTaskPluginCollector();
        }

        @Override
        public void destroy() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            String peerPluginName = this.writerSliceConfig.getString(PEER_PLUGIN_NAME);
            LOG.debug("Start to handle record from: " + peerPluginName);

            // 业务逻辑: DataHandler
            DataHandler handler = new DefaultDataHandler(this.writerSliceConfig, this.taskPluginCollector);

            long records = handler.handle(recordReceiver, getTaskPluginCollector());
            LOG.debug("Finish handling data, records: " + records);
        }
    }
}
