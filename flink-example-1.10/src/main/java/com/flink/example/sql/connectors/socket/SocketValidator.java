package com.flink.example.sql.connectors.socket;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

/**
 * 功能：Socket Source Validator
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/11/1 12:39
 */
public class SocketValidator extends ConnectorDescriptorValidator {
    public static final String CONNECTOR_TYPE_VALUE = "socket";
    public static final String CONNECTOR_HOST = "host";
    public static final String CONNECTOR_PORT = "port";
    public static final String CONNECTOR_DELIMITER = "delimiter";
    public static final String CONNECTOR_MAX_NUM_RETRIES = "maxNumRetries";
    public static final String CONNECTOR_DELAY_BETWEEN_RETRIES = "delayBetweenRetries";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        // 必填参数校验
        properties.validateString(CONNECTOR_HOST, false, 1);
        properties.validateInt(CONNECTOR_PORT, false);
        properties.validateString(CONNECTOR_DELIMITER, true, 1);

        // 可选参数校验
        Optional<Long> maxNumRetries = properties.getOptionalLong(CONNECTOR_MAX_NUM_RETRIES);
        maxNumRetries.ifPresent(num -> Preconditions.checkArgument(
                num >= -1,
                "maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)"
        ));
        Optional<Long> delayBetweenRetries = properties.getOptionalLong(CONNECTOR_DELAY_BETWEEN_RETRIES);
        delayBetweenRetries.ifPresent(delay -> Preconditions.checkArgument(delay >= 0, "delayBetweenRetries must be zero or positive"));
    }
}
