package com.flink.example.sql.connectors.custom;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.TableSource;

/**
 * 功能：示例
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/11/2 23:42
 */
public class FileSystemTableSource implements ProjectableTableSource<RowData> {
    @Override
    public TableSource<RowData> projectFields(int[] ints) {
        return null;
    }
}
