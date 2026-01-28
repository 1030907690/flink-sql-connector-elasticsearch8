package com.zzq.flink.streaming.connectors.elasticsearch.table;

import com.zzq.flink.streaming.connectors.elasticsearch.config.ElasticsearchOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author zzq
 * @date 2026/01/27 18:48:39
 * 入口点。负责解析 SQL 中的 WITH 参数，并决定创建 Sink 还是 Source
 */
public class ElasticsearchDynamicTableFactory implements DynamicTableSinkFactory {



    @Override
    public String factoryIdentifier() {
        return "elasticsearch-8-custom"; // SQL 中使用的 'connector' 名称
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ElasticsearchOptions.HOSTS); // 必须添加到这里
        options.add(ElasticsearchOptions.INDEX);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ElasticsearchOptions.USERNAME);
        options.add(ElasticsearchOptions.PASSWORD);
        options.add(ElasticsearchOptions.BULK_FLUSH_MAX_ACTIONS);
        options.add(ElasticsearchOptions.CA_FINGERPRINT);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        // 1. 获取配置
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();

        // 2. 校验配置
        helper.validate();

        // 3. 获取物理数据类型（Schema）
        DataType physicalDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        return new ElasticsearchDynamicSink(config, physicalDataType);
    }
}