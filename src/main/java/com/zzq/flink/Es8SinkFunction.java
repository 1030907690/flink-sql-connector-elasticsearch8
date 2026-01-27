package com.zzq.flink;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zzq
 * @date 2026/01/27 18:51:19
 */
/*
public class Es8SinkFunction extends RichSinkFunction<RowData> {
    private transient ElasticsearchClient client;
    private final String hosts;
    private final String index;
    private final DataType dataType;

    public Es8SinkFunction(String hosts, String index, DataType dataType) {
        this.hosts = hosts;
        this.index = index;
        this.dataType = dataType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 ES 8 客户端
        RestClient restClient = RestClient.builder(HttpHost.create(hosts)).build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        // 1. 将 RowData 转换为 Map 或 JSON 字符串
        // 这里可以使用 Flink 提供的 RowData.FieldGetter

        // 2. 发起 ES 索引请求
        client.index(i -> i
                .index(index)
                .document(value.toString()) // 简化示例，实际需按 schema 转换
        );
    }

    @Override
    public void close() throws Exception {
        if (client != null) { */
/* 关闭客户端 *//*
 }
    }
}*/
public class Es8SinkFunction extends RichSinkFunction<RowData> {
    private transient ElasticsearchClient client;
    private final String hosts;
    private final String index;
    private final DataType physicalDataType;
    private transient RowData.FieldGetter[] fieldGetters;
    private int primaryKeyIndex = 0; // 假设第一列是主键，实际应从 Schema 获取

    public Es8SinkFunction(String hosts, String index, DataType physicalDataType) {
        this.hosts = hosts;
        this.index = index;
        this.physicalDataType = physicalDataType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 1. 初始化 ES 8 客户端
        RestClient restClient = RestClient.builder(HttpHost.create(hosts)).build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);

        // 2. 预编译字段提取器 (提高性能)
        LogicalType logicalType = physicalDataType.getLogicalType();
        RowType rowType = (RowType) logicalType;
        fieldGetters = new RowData.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldGetters[i] = RowData.createFieldGetter(rowType.getTypeAt(i), i);
        }
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        // 获取当前行操作类型
        RowKind kind = value.getRowKind();
        String docId = fieldGetters[primaryKeyIndex].getFieldOrNull(value).toString();

        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            // Upsert 操作
            Map<String, Object> doc = rowToMap(value);
            client.index(i -> i
                    .index(index)
                    .id(docId)
                    .document(doc)
            );
        } else if (kind == RowKind.DELETE) {
            // 删除操作
            client.delete(d -> d.index(index).id(docId));
        }
        // UPDATE_BEFORE 通常忽略，因为紧接着的 UPDATE_AFTER 会覆盖整个 Doc
    }

    private Map<String, Object> rowToMap(RowData row) {
        Map<String, Object> map = new HashMap<>();
        RowType rowType = (RowType) physicalDataType.getLogicalType();
        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldGetters.length; i++) {
            map.put(fieldNames.get(i), fieldGetters[i].getFieldOrNull(row));
        }
        return map;
    }

    @Override
    public void close() throws Exception {
        if (client != null) client._transport().close();
    }
}

