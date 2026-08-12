package com.zzq.flink.streaming.connectors.elasticsearch.table;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
// 批量写入器：自动攒批、自动按 条数/字节数/时间 触发发送（8.15+ 版本新增，位于 _helpers 辅助包）
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
// 批量监听器：发送前/后回调，用于日志与失败处理（注意：此版本要求实现全部 3 个方法）
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
// 批量响应中单条操作的结果（可能部分成功部分失败，需要逐个检查）
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 真正操作ES的地方
 * @author zzq
 * @since 2026/01/27 18:51:19
 *
 */
public class ElasticsearchSinkFunction extends RichSinkFunction<RowData> {

    private final Logger log = LoggerFactory.getLogger(ElasticsearchSinkFunction.class);

    private transient ElasticsearchClient client;
    // 批量写入器：invoke() 只负责把操作加进缓冲，真正发请求由 BulkIngester 内部线程完成
    private transient BulkIngester<Void> bulkIngester;
    private final String hosts;
    private final String index;
    private final DataType physicalDataType;
    // ===== 批量写入参数（由构造器从配置传入，序列化到 TaskManager 后仍可用）=====
    private final int bulkMaxActions;              // 每批最大条数，攒够即发
    private final long bulkMaxSizeBytes;           // 每批最大字节数，攒够即发
    private final long bulkFlushIntervalMillis;    // 攒批最长等待时间，到点即发（保证实时性）
    private final int bulkConcurrentRequests;      // 并发 bulk 请求数，超出阻塞 add() 形成背压
    private transient RowData.FieldGetter[] fieldGetters;
    /**
     *  假设第一列是主键，实际应从 Schema 获取
     * */
    private int primaryKeyIndex = 0;
    /***
     * 在类成员变量中定义格式化器
     * **/
    private transient java.time.format.DateTimeFormatter formatter;

    /**
     * @param hosts                    ES 地址，支持逗号分隔多节点，如 "http://es1:9200,http://es2:9200"
     * @param index                    目标索引名
     * @param physicalDataType         物理表结构（用于把 RowData 转成 Map）
     * @param bulkMaxActions           每批最大条数（sink.bulk-flush.max-actions）
     * @param bulkMaxSizeBytes         每批最大字节数（sink.bulk-flush.max-size）
     * @param bulkFlushIntervalMillis  攒批最长时间 ms（sink.bulk-flush.interval）
     * @param bulkConcurrentRequests   并发 bulk 请求数（sink.bulk-concurrent-requests）
     */
    public ElasticsearchSinkFunction(String hosts, String index, DataType physicalDataType,
                                     int bulkMaxActions, long bulkMaxSizeBytes,
                                     long bulkFlushIntervalMillis, int bulkConcurrentRequests) {
        this.hosts = hosts;
        this.index = index;
        this.physicalDataType = physicalDataType;
        this.bulkMaxActions = bulkMaxActions;
        this.bulkMaxSizeBytes = bulkMaxSizeBytes;
        this.bulkFlushIntervalMillis = bulkFlushIntervalMillis;
        this.bulkConcurrentRequests = bulkConcurrentRequests;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        // 初始化 ES 8 客户端（支持逗号分隔的多节点 + 连接池调优）
        // 多节点：RestClient 会自动做负载均衡，节点挂了自动切换，吞吐更高
        HttpHost[] httpHosts = Arrays.stream(hosts.split(","))
                .map(String::trim)      // 去掉逗号前后的空格
                .map(HttpHost::create)
                .toArray(HttpHost[]::new);
        RestClient restClient = RestClient.builder(httpHosts)
                // 调大连接池：默认连接数偏保守，高并发写入时不够用
                .setHttpClientConfigCallback(cb -> cb
                        .setMaxConnTotal(200)     // 总连接数上限
                        .setMaxConnPerRoute(50))  // 每个节点连接数上限
                .build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);

        // 预编译字段提取器 (提高性能)
        LogicalType logicalType = physicalDataType.getLogicalType();
        RowType rowType = (RowType) logicalType;
        fieldGetters = new RowData.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldGetters[i] = RowData.createFieldGetter(rowType.getTypeAt(i), i);
        }

        // ============ 初始化批量写入器（性能优化的核心）============
        // 攒批触发条件（满足任一即发送）：
        //   1. maxOperations：缓冲的操作数达到 bulkMaxActions 条
        //   2. maxSize：缓冲的操作字节数达到 bulkMaxSizeBytes
        //   3. flushInterval：距上次发送超过 bulkFlushIntervalMillis（低流量时保证实时性）
        // maxConcurrentRequests：允许同时有 N 个 bulk 请求在途（异步）；
        //   达到上限后 add() 会阻塞，从而把背压传递给 Flink 上游，防止 ES 过载
        this.bulkIngester = BulkIngester.<Void>of(b -> b
                .client(client)
                .maxOperations(bulkMaxActions)
                .maxSize(bulkMaxSizeBytes)
                .flushInterval(bulkFlushIntervalMillis, TimeUnit.MILLISECONDS)
                .maxConcurrentRequests(bulkConcurrentRequests)
                // 监听器：用于日志和失败处理（三个方法都必须实现）
                .listener(new BulkListener<Void>() {

                    /**
                     * 批量请求发送前回调，一般用于监控/日志
                     */
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request, List<Void> contexts) {
                        log.debug("bulk request preparing, executionId={}, actions={}",
                                executionId, request.operations().size());
                    }

                    /**
                     * 批量请求正常返回后回调（HTTP 层成功，但内部可能有部分操作失败）
                     * 例如：某条文档字段类型与 mapping 冲突、索引不存在等，
                     * 此时 response.items() 中对应条目的 error() 不为 null
                     */
                    @Override
                    public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, BulkResponse response) {
                        // 逐个检查每一条操作的结果，把失败的打印出来方便排查
                        for (BulkResponseItem item : response.items()) {
                            if (item.error() != null) {
                                log.error("bulk item failed, id={}, reason={}", item.id(), item.error().reason());
                            }
                        }
                    }

                    /**
                     * 批量请求整体失败回调（网络异常、ES 集群不可用等）
                     * 处理：把整个批次重新 add 回去，实现 at-least-once（最少一次）投递
                     * 注意：本方案是简单重试，ES 长时间不可用会无限重加导致堆积，
                     *       生产环境建议在此加最大重试次数或退避策略
                     */
                    @Override
                    public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, Throwable failure) {
                        log.error("bulk request failed, retrying, actions={}", request.operations().size(), failure);
                        // 把失败的整个批次重新加入队列，等待下一轮发送
                        request.operations().forEach(bulkIngester::add);
                    }
                }));
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        // 获取当前行操作类型
        RowKind kind = value.getRowKind();
        String docId = fieldGetters[primaryKeyIndex].getFieldOrNull(value).toString();

        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            log.info("write ES, index={}, docId={}, op={}", index, docId, kind);
            // Upsert 操作（加入批量缓冲，不阻塞当前线程，吞吐远高于逐条写）
            Map<String, Object> doc = rowToMap(value);
            bulkIngester.add(op -> op.index(i -> i
                    .index(index)
                    .id(docId)
                    .document(doc)
            ));

            log.debug("write ES success, index={}, docId={}, doc={}", index, docId, doc);

        } else if (kind == RowKind.DELETE) {
            // 删除操作（同样进入批量，与写入共用同一缓冲队列）
            bulkIngester.add(op -> op.delete(d -> d.index(index).id(docId)));
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
        // 关闭顺序很重要：先 flush 掉缓冲区里还没发出去的数据，再关连接
        if (bulkIngester != null) {
            bulkIngester.close();  // 会等待剩余操作发送完成并释放内部线程资源
        }
        if (client != null) {
            client._transport().close();
        }
    }
}

