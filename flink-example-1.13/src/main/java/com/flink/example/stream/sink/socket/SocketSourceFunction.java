package com.flink.example.stream.sink.socket;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 功能：自定义 Socket Source Function
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/11/1 15:53
 */
public class SocketSourceFunction extends RichSourceFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SocketSourceFunction.class);
    private static final int DEFAULT_CONNECTION_RETRY_SLEEP = 500;
    private static final int CONNECTION_TIMEOUT_TIME = 0;
    private final String host;
    private final int port;
    private final String delimiter;
    private final long maxNumRetries;
    private final long delayBetweenRetries;
    private transient Socket currentSocket;
    private volatile boolean isRunning;

    public SocketSourceFunction(String host, int port, String delimiter, long maxNumRetries) {
        this(host, port, delimiter, maxNumRetries, DEFAULT_CONNECTION_RETRY_SLEEP);
    }

    public SocketSourceFunction(String host, int port, String delimiter, long maxNumRetries, long delayBetweenRetries) {
        this.isRunning = true;
        Preconditions.checkArgument(NetUtils.isValidClientPort(port), "port is out of range");
        Preconditions.checkArgument(maxNumRetries >= -1L, "maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)");
        Preconditions.checkArgument(delayBetweenRetries >= 0L, "delayBetweenRetries must be zero or positive");
        this.host = Preconditions.checkNotNull(host, "hostname must not be null");
        this.port = port;
        this.delimiter = delimiter;
        this.maxNumRetries = maxNumRetries;
        this.delayBetweenRetries = delayBetweenRetries;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        StringBuilder buffer = new StringBuilder();
        long attempt = 0L;

        while(this.isRunning) {
            try (Socket socket = new Socket()) {
                this.currentSocket = socket;
                LOG.info("Connecting to server socket {}:{}", this.host, this.port);
                socket.connect(new InetSocketAddress(this.host, this.port), CONNECTION_TIMEOUT_TIME);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                Throwable var8 = null;

                try {
                    char[] cbuf = new char[8192];
                    int bytesRead;
                    while(this.isRunning && (bytesRead = reader.read(cbuf)) != -1) {
                        buffer.append(cbuf, 0, bytesRead);
                        int delimPos;
                        while(buffer.length() >= this.delimiter.length() && (delimPos = buffer.indexOf(this.delimiter)) != -1) {
                            String record = buffer.substring(0, delimPos);
                            if (this.delimiter.equals("\n") && record.endsWith("\r")) {
                                record = record.substring(0, record.length() - 1);
                            }
                            ctx.collect(record);
                            buffer.delete(0, delimPos + this.delimiter.length());
                        }
                    }
                } catch (Throwable var34) {
                    var8 = var34;
                    throw var34;
                } finally {
                    if (reader != null) {
                        if (var8 != null) {
                            try {
                                reader.close();
                            } catch (Throwable var33) {
                                var8.addSuppressed(var33);
                            }
                        } else {
                            reader.close();
                        }
                    }

                }
            }

            // Socket 断链重试
            if (this.isRunning) {
                ++attempt;
                // 最多重试 maxNumRetries 次
                if (this.maxNumRetries != -1L && attempt >= this.maxNumRetries) {
                    break;
                }
                LOG.warn("Lost connection to server socket. Retrying in {} msecs...", this.delayBetweenRetries);
                Thread.sleep(this.delayBetweenRetries);
            }
        }
        // 输出
        if (buffer.length() > 0) {
            ctx.collect(buffer.toString());
        }

    }

    public void cancel() {
        this.isRunning = false;
        Socket theSocket = this.currentSocket;
        if (theSocket != null) {
            IOUtils.closeSocket(theSocket);
        }

    }
}
