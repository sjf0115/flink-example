package com.flink.example.sql.connectors.custom;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * 功能：示例
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/11/2 23:25
 */
public class TimeAttributeTableSource implements StreamTableSource<Row>, DefinedProctimeAttribute, DefinedRowtimeAttributes {
    private final Optional<String> procTimeAttribute;
    private final List<RowtimeAttributeDescriptor> rowTimeAttributeDescriptors;

    public TimeAttributeTableSource(Optional<String> procTimeAttribute, List<RowtimeAttributeDescriptor> rowTimeAttributeDescriptors) {
        this.procTimeAttribute = procTimeAttribute;
        this.rowTimeAttributeDescriptors = rowTimeAttributeDescriptors;
    }

    // StreamTableSource
    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }

    @Override
    public DataType getProducedDataType() {
        return StreamTableSource.super.getProducedDataType();
    }

    // DefinedProctimeAttribute
    @Nullable
    @Override
    public String getProctimeAttribute() {
        return procTimeAttribute.orElse(null);
    }

    // DefinedRowtimeAttributes
    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        return rowTimeAttributeDescriptors;
    }
}
