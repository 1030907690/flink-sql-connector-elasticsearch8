package com.zzq.flink.streaming.connectors.elasticsearch.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

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

    // ES8 特有：CA 证书指纹 (用于自签名证书校验)
    public static final ConfigOption<String> CA_FINGERPRINT = ConfigOptions
            .key("ssl.ca-fingerprint")
            .stringType()
            .noDefaultValue()
            .withDescription("The SHA-256 fingerprint of the CA certificate.");
}