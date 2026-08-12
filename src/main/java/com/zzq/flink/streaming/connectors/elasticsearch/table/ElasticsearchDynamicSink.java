package com.zzq.flink.streaming.connectors.elasticsearch.table;


import com.zzq.flink.streaming.connectors.elasticsearch.config.ElasticsearchOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

/**
 * 描述数据如何写入。负责在提交 Job 时验证 Schema 和配置
 * @author zzq
 * @since 2026/01/27 18:50:21
 */

public class ElasticsearchDynamicSink implements DynamicTableSink {


    private final DataType physicalDataType;
    private final ReadableConfig config;

    /**
     * 构造函数，由 Factory 调用
     *
     * @param config            SQL WITH 中解析出的全部配置项
     * @param physicalDataType  物理表结构（排除计算列等非物理列）
     */
    public ElasticsearchDynamicSink(ReadableConfig config, DataType physicalDataType) {
        this.config = config;
        this.physicalDataType = physicalDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // 告知 Flink 本 Sink 支持的数据操作类型
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
        // 批量写入参数在这里从配置中取出，通过构造函数传给 SinkFunction：
        //  - 不直接传 ReadableConfig：SinkFunction 实例会被 Java 序列化分发到各 TaskManager，
        //    传基本类型最稳妥（无序列化风险）
        //  - MemorySize/Duration 是 Flink 配置类型，转成基本类型（long）后传递
        ElasticsearchSinkFunction sinkFunction = new ElasticsearchSinkFunction(config.get(ElasticsearchOptions.HOSTS),
                config.get(ElasticsearchOptions.INDEX), physicalDataType,
                config.get(ElasticsearchOptions.BULK_FLUSH_MAX_ACTIONS),       // 每条批次最大条数（int）
                config.get(ElasticsearchOptions.BULK_FLUSH_MAX_SIZE).getBytes(),      // 每批最大字节数（long）
                config.get(ElasticsearchOptions.BULK_FLUSH_INTERVAL).toMillis(),      // 攒批最长时间（毫秒）
                config.get(ElasticsearchOptions.BULK_CONCURRENT_REQUESTS));   // 并发 bulk 请求数（int）

        // 返回 SinkFunctionProvider，Flink 会在并行算子中实例化它
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new ElasticsearchDynamicSink(config, physicalDataType);
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch 8 Sink";
    }
}