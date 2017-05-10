package org.apache.phoenix.transaction;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.apache.omid.transaction.HBaseTransactionManager;
import org.apache.omid.transaction.RollbackException;
import org.apache.omid.transaction.TTable;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionException;
import org.apache.omid.transaction.TransactionManager;


public class OmidTransactionContext implements PhoenixTransactionContext {

    private Transaction tx;
    private TransactionManager tm;
    
    public OmidTransactionContext() {
//        try (TransactionManager tm = HBaseTransactionManager.newInstance();
//                TTable txTable = new TTable("MY_TX_TABLE")) {
//
//               Transaction tx = tm.begin();

//               Put row1 = new Put(Bytes.toBytes("EXAMPLE_ROW1"));
//               row1.add(family, qualifier, Bytes.toBytes("val1"));
//               txTable.put(tx, row1);
//
//               Put row2 = new Put(Bytes.toBytes("EXAMPLE_ROW2"));
//               row2.add(family, qualifier, Bytes.toBytes("val2"));
//               txTable.put(tx, row2);

//               tm.commit(tx);

//           }
    }

    @Override
    public void begin() throws SQLException {
        if (tm == null) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.NULL_TRANSACTION_CONTEXT).build()
                    .buildException();
        }

        try {
            tx = tm.begin();
        } catch (TransactionException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }
    }

    @Override
    public void commit() throws SQLException {
        // TODO Auto-generated method stub

    }

    
    @Override
    public void abort() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void checkpoint(boolean hasUncommittedData) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void commitDDLFence(PTable dataTable, Logger logger) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void join(PhoenixTransactionContext ctx) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isTransactionRunning() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void reset() {
        // TODO Auto-generated method stub

    }

    @Override
    public long getTransactionId() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getReadPointer() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getWritePointer() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public PhoenixVisibilityLevel getVisibilityLevel() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setVisibilityLevel(PhoenixVisibilityLevel visibilityLevel) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public byte[] encodeTransaction() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getMaxTransactionsPerSecond() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isPreExistingVersion(long version) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public BaseRegionObserver getCoProcessor() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setInMemoryTransactionClient(Configuration config) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public ZKClientService setTransactionClient(Configuration config, ReadOnlyProps props,
            ConnectionInfo connectionInfo) {
        // TODO Auto-generated method stub
        
        return null;
        
    }

    @Override
    public byte[] getFamilyDeleteMarker() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setTxnConfigs(Configuration config, String tmpFolder, int defaultTxnTimeoutSeconds) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setupTxManager(Configuration config, String url) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void tearDownTxManager() {
        // TODO Auto-generated method stub

    }
}
