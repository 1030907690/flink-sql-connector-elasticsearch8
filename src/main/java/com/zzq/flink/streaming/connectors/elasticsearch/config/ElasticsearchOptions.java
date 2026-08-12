package com.zzq.flink.streaming.connectors.elasticsearch.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import java.time.Duration;

/**
 * 集中管理所有 ConfigOption
 * @author zzq
 * @since 2026/01/28 15:59:30
 *
 */
public class ElasticsearchOptions {

    // 必填参数：集群地址
    public static final ConfigOption<String> HOSTS = ConfigOptions
            .key("hosts")
            .stringType()
            .noDefaultValue()
            .withDescription("Elasticsearch hosts (e.g., 'https://localhost:9200').");

    // 必填参数：索引名称
    public static final ConfigOption<String> INDEX = ConfigOptions
            .key("index")
            .stringType()
            .noDefaultValue()
            .withDescription("The index to write to.");

    // 选填参数：用户名
    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional username for Basic Auth.");

    // 选填参数：密码 (注意使用 password 类型，日志中会脱敏)
    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional password for Basic Auth.");

    // 性能参数：批量写入条数
    public static final ConfigOption<Integer> BULK_FLUSH_MAX_ACTIONS = ConfigOptions
            .key("sink.bulk-flush.max-actions")
            .intType()
            .defaultValue(1000)
            .withDescription("Maximum number of actions to buffer per bulk request.");

    // 性能参数：每个批量请求的最大字节数
    // 攒批时同时看条数和字节数，先达到哪个就触发发送
    // 例如：数据单条很大（如 1KB+），可能攒 100 条就 1MB 了，此时按字节触发更合理
    public static final ConfigOption<MemorySize> BULK_FLUSH_MAX_SIZE = ConfigOptions
            .key("sink.bulk-flush.max-size")
            .memoryType()
            .defaultValue(MemorySize.ofMebiBytes(5))
            .withDescription("Maximum size of buffered operations per bulk request.");

    // 性能参数：攒批最长时间（保证低延迟）
    // 低流量时可能很久攒不够 1000 条，若无限等待会导致数据延迟过大
    // 到达该时间间隔后，即使没攒够也会触发发送，保证数据实时性
    public static final ConfigOption<Duration> BULK_FLUSH_INTERVAL = ConfigOptions
            .key("sink.bulk-flush.interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(1))
            .withDescription("Flush buffered requests if the time since the last flush exceeds this.");

    // 性能参数：并发 bulk 请求数
    // 允许同时有多个 bulk 请求在途（异步发送），充分利用带宽和 ES 处理能力
    // 注意：超出该数量时 add() 会阻塞，这是刻意的背压机制，防止 ES 被打垮
    public static final ConfigOption<Integer> BULK_CONCURRENT_REQUESTS = ConfigOptions
            .key("sink.bulk-concurrent-requests")
            .intType()
            .defaultValue(3)
            .withDescription("Maximum number of concurrent bulk requests.");

    // ES8 特有：CA 证书指纹 (用于自签名证书校验)
    public static final ConfigOption<String> CA_FINGERPRINT = ConfigOptions
            .key("ssl.ca-fingerprint")
            .stringType()
            .noDefaultValue()
            .withDescription("The SHA-256 fingerprint of the CA certificate.");
}