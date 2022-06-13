package com.yankee.sink;

import com.yankee.bean.Event;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(BufferingSink.class);

    private final int threshold;
    private transient ListState<Event> checkpointedState;
    private List<Event> bufferedElements;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void invoke(Event value, Context context) throws Exception {
        bufferedElements.add(value);
        if (bufferedElements.size() == threshold) {
            for (Event element : bufferedElements) {
                // 输出到外部系统
                LOG.info("结果数据是：{}", element);
            }
            LOG.info("输出完毕！");
            bufferedElements.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        // 把当前局部变量中的所有元素写入检查点中
        for (Event element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-elements", Types.POJO(Event.class));
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        // 如果是故障恢复，就让ListState中的所有元素添加到局部变量中
        if (context.isRestored()) {
            for (Event element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }
}
