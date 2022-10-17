package com.alibaba.datax.plugin.writer.nebulagraphwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;

public interface DataHandler {
    int handle(RecordReceiver rec, TaskPluginCollector collector);
}
