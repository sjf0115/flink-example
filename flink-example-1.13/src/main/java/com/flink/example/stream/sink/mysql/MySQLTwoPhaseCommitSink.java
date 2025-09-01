package com.flink.example.stream.sink.mysql;

import jdk.nashorn.internal.ir.ObjectNode;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;

public class MySQLTwoPhaseCommitSink<IN> extends TwoPhaseCommitSinkFunction<IN, Connection, Void> {

    public MySQLTwoPhaseCommitSink(TypeSerializer<Connection> transactionSerializer, TypeSerializer<Void> contextSerializer) {
        super(transactionSerializer, contextSerializer);
    }

    @Override
    protected void invoke(Connection connection, IN objectNode, Context context) throws Exception {

    }

    @Override
    protected Connection beginTransaction() throws Exception {
        return null;
    }

    @Override
    protected void preCommit(Connection connection) throws Exception {

    }

    @Override
    protected void commit(Connection connection) {

    }

    @Override
    protected void abort(Connection connection) {

    }
}
