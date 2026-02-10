package com.zzq.flink.streaming.connectors.elasticsearch.table;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.*;
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
 * 真正操作ES的地方
 * @author zzq
 * @since 2026/01/27 18:51:19
 *
 */
public class ElasticsearchSinkFunction extends RichSinkFunction<RowData> {
    private transient ElasticsearchClient client;
    private final String hosts;
    private final String index;
    private final DataType physicalDataType;
    private transient RowData.FieldGetter[] fieldGetters;
    private int primaryKeyIndex = 0; // 假设第一列是主键，实际应从 Schema 获取
    // 在类成员变量中定义格式化器
    private transient java.time.format.DateTimeFormatter formatter;

    public ElasticsearchSinkFunction(String hosts, String index, DataType physicalDataType) {
        this.hosts = hosts;
        this.index = index;
        this.physicalDataType = physicalDataType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        // 初始化 ES 8 客户端
        RestClient restClient = RestClient.builder(HttpHost.create(hosts)).build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);

        // 预编译字段提取器 (提高性能)
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
            Object val = fieldGetters[i].getFieldOrNull(row);
            // 获取该列的逻辑类型，用于处理复杂类型
            LogicalType type = rowType.getTypeAt(i);
            map.put(fieldNames.get(i), convertFlinkType(val, type));
        }
        return map;
    }

    private Object convertFlinkType(Object val, LogicalType type) {
        if (val == null) {
            return null;
        }

        // 处理嵌套行 (NestedRowData)
        if (val instanceof RowData) {
            RowData row = (RowData) val;
            RowType rowType = (RowType) type;
            Map<String, Object> nestedMap = new HashMap<>();
            List<String> fieldNames = rowType.getFieldNames();
            List<LogicalType> fieldTypes = rowType.getChildren();

            for (int i = 0; i < row.getArity(); i++) {
                // 为每一列创建临时 FieldGetter
                RowData.FieldGetter getter = RowData.createFieldGetter(fieldTypes.get(i), i);
                nestedMap.put(fieldNames.get(i), convertFlinkType(getter.getFieldOrNull(row), fieldTypes.get(i)));
            }
            return nestedMap;
        }

        // 处理 Map 类型
        if (val instanceof MapData) {
            MapData mapData = (MapData) val;
            LogicalType keyType = ((org.apache.flink.table.types.logical.MapType) type).getKeyType();
            LogicalType valueType = ((org.apache.flink.table.types.logical.MapType) type).getValueType();

            // 提取 Key 和 Value 的 FieldGetter (简单示例，生产建议缓存)
            ArrayData keyArray = mapData.keyArray();
            ArrayData valueArray = mapData.valueArray();
            Map<Object, Object> javaMap = new HashMap<>();

            for (int i = 0; i < mapData.size(); i++) {
                Object k = ArrayData.createElementGetter(keyType).getElementOrNull(keyArray, i);
                Object v = ArrayData.createElementGetter(valueType).getElementOrNull(valueArray, i);
                javaMap.put(convertFlinkType(k, keyType), convertFlinkType(v, valueType));
            }
            return javaMap;
        }

        // 处理 Array 类型
        if (val instanceof ArrayData) {
            ArrayData arrayData = (ArrayData) val;
            LogicalType eleType = ((org.apache.flink.table.types.logical.ArrayType) type).getElementType();
            List<Object> list = new java.util.ArrayList<>();
            for (int i = 0; i < arrayData.size(); i++) {
                Object ele = ArrayData.createElementGetter(eleType).getElementOrNull(arrayData, i);
                list.add(convertFlinkType(ele, eleType));
            }
            return list;
        }


        // 处理基础 Data 包装类
        if (val instanceof StringData) {
            return val.toString();
        }
        if (val instanceof TimestampData) {
            // 解决时间格式
            return ((TimestampData) val).toLocalDateTime().format(formatter);
        }
        if (val instanceof DecimalData) {
            return ((DecimalData) val).toBigDecimal();
        }
        return val;
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client._transport().close();
        }
    }
}

