package com.zzq.flink;


import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

/**
 * @author zzq
 * @date 2026/01/27 18:50:21
 */

public class Es8DynamicSink implements DynamicTableSink {

    private final String hosts;
    private final String index;
    private final DataType physicalDataType;

    // 构造函数，由 Factory 调用
    public Es8DynamicSink(String hosts, String index, DataType physicalDataType) {
        this.hosts = hosts;
        this.index = index;
        this.physicalDataType = physicalDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // 告知 Flink 本 Sink 支持的数据操作类型
        // 简单实现可以只支持 INSERT，如果需要支持 UPDATE/DELETE，需要在这里配置
     /*   return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();*/
        // 支持所有操作类型：插入、更新前、更新后、删除
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // 核心步骤：创建真正的物理 Sink 执行器
        // 我们可以把解析后的 physicalDataType 传递给 SinkFunction
        Es8SinkFunction sinkFunction = new Es8SinkFunction(hosts, index, physicalDataType);

        // 返回 SinkFunctionProvider，Flink 会在并行算子中实例化它
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new Es8DynamicSink(hosts, index, physicalDataType);
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch 8 Custom Sink";
    }
}