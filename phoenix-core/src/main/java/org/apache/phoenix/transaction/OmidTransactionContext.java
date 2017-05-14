package org.apache.phoenix.transaction;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.apache.omid.transaction.AbstractTransactionManager;
import org.apache.omid.transaction.HBaseCellId;
import org.apache.omid.transaction.HBaseTransaction;
import org.apache.omid.transaction.HBaseTransactionManager;
import org.apache.omid.transaction.RollbackException;
import org.apache.omid.transaction.TTable;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionException;
import org.apache.omid.transaction.TransactionManager;

import com.beust.jcommander.internal.Lists;


public class OmidTransactionContext implements PhoenixTransactionContext {

    private static TransactionManager transactionManager = null;

    private TransactionManager tm;
    private Transaction tx;

    public OmidTransactionContext() {
        this.tx = null;
        this.tm = null;
    }

    public OmidTransactionContext(PhoenixConnection connection) {
        this.tm = transactionManager;
        this.tx = null;
    }

    public OmidTransactionContext(byte[] txnBytes) {
        
    }
 
    public OmidTransactionContext(PhoenixTransactionContext ctx,
            PhoenixConnection connection, boolean subTask) {

        this.tm = transactionManager;
        
        assert (ctx instanceof OmidTransactionContext);
        OmidTransactionContext omidTransactionContext = (OmidTransactionContext) ctx;
        
        
        if (subTask) {
            Transaction transaction = omidTransactionContext.getTransaction();
            this.tx = new HBaseTransaction(transaction.getTransactionId(), transaction.getEpoch(), new HashSet<HBaseCellId>(), null);
            this.tm = null;
        } else {
            this.tx = omidTransactionContext.getTransaction();
        }
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
        if (tx == null || tm == null)
            return;

        try {
            tm.commit(tx);
        } catch (TransactionException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        } catch (RollbackException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }
    }

    @Override
    public void abort() throws SQLException {
        if (tx == null || tm == null)
            return;

        try {
            tm.rollback(tx);
        } catch (TransactionException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }
        
    }

    @Override
    public void checkpoint(boolean hasUncommittedData) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void commitDDLFence(PTable dataTable, Logger logger) throws SQLException {
        // TODO Auto-generated method stub

    }

    public void markDMLFence(PTable table) {
        
    }

    @Override
    public void join(PhoenixTransactionContext ctx) {
        assert (ctx instanceof OmidTransactionContext);
        OmidTransactionContext omidContext = (OmidTransactionContext) ctx;
        
        assert (omidContext.getTransaction() instanceof HBaseTransaction);
        Set<HBaseCellId> writeSet = ((HBaseTransaction) omidContext.getTransaction()).getWriteSet();
        
        assert (tx instanceof HBaseTransaction);
        HBaseTransaction hbaseTx = (HBaseTransaction) tx;
        
        for (HBaseCellId cell : writeSet) {
            hbaseTx.addWriteSetElement(cell);
        }
    }

    @Override
    public boolean isTransactionRunning() {
        return (tx != null);
    }

    @Override
    public void reset() {
        tx = null;
    }

    @Override
    public long getTransactionId() {
        return tx.getTransactionId();
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
        try {
            transactionManager = HBaseTransactionManager.newInstance();
        } catch (IOException | InterruptedException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }
    }

    @Override
    public void tearDownTxManager() throws SQLException {
        try {
            tm.close();
        } catch (IOException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();        }
    }

    /**
     *  OmidTransactionContext specific functions 
     */

    public Transaction getTransaction() {
        return tx;
    }

    public List<OmidTransactionTable> getTransactionalTables() {
        return transactionalTables;
    }
}
