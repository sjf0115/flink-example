package com.flink.example.stream.sink.commit;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public abstract class TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT> extends RichSinkFunction<IN>
        implements CheckpointedFunction, CheckpointListener {

    // -----------------------------------------------------------------------------------------------------------------
    // 实现 CheckpointListener 接口重写 notifyCheckpointComplete 方法

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {

    }

    // -----------------------------------------------------------------------------------------------------------------
    // 实现 CheckpointedFunction 接口重写 snapshotState 和 initializeState 方法

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    // -----------------------------------------------------------------------------------------------------------------
    // 实现 RichSinkFunction 接口重写 invoke 方法

    @Override
    public void invoke(IN value) throws Exception {
        super.invoke(value);
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        super.invoke(value, context);
    }
}
