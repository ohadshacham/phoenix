package org.apache.phoenix.transaction;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.omid.transaction.TTable;
import org.apache.omid.transaction.Transaction;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.PTable;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

public class OmidTransactionTable implements PhoenixTransactionalTable {

    private TTable tTable;
    private Transaction tx;
    
    public OmidTransactionTable(PhoenixTransactionContext ctx, HTableInterface hTable) throws SQLException {
        this(ctx, hTable, null);
    }

    public OmidTransactionTable(PhoenixTransactionContext ctx, HTableInterface hTable, PTable pTable) throws SQLException  {
        assert(ctx instanceof OmidTransactionContext);

        OmidTransactionContext omidTransactionContext = (OmidTransactionContext) ctx;

        try {
            tTable = new TTable(hTable);
        } catch (IOException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
            .setMessage(e.getMessage()).setRootCause(e).build()
            .buildException();
        }

        this.tx = omidTransactionContext.getTransaction();

        if (pTable != null && pTable.getType() != PTableType.INDEX) {
            omidTransactionContext.markDMLFence(pTable);
        }
    }

    @Override
    public Result get(Get get) throws IOException {
        return tTable.get(tx, get);
    }

    @Override
    public void put(Put put) throws IOException {
        tTable.put(tx, put);

    }

    @Override
    public void delete(Delete delete) throws IOException {
        tTable.delete(tx, delete);
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
       return tTable.getScanner(tx, scan);
    }

    @Override
    public byte[] getTableName() {
        return tTable.getTableName();
    }

    @Override
    public Configuration getConfiguration() {
        return tTable.getConfiguration();
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return tTable.getTableDescriptor();
    }

    @Override
    public boolean exists(Get get) throws IOException {
       return tTable.exists(tx, get);
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        return tTable.get(tx, gets);
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        return tTable.getScanner(tx, family);
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier)
            throws IOException {
        return tTable.getScanner(tx, family, qualifier);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        tTable.put(tx, puts);
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        tTable.delete(tx, deletes);
    }

    @Override
    public void setAutoFlush(boolean autoFlush) {
        tTable.setAutoFlush(autoFlush);
    }

    @Override
    public boolean isAutoFlush() {
        return tTable.isAutoFlush();
    }

    @Override
    public long getWriteBufferSize() {
        return tTable.getWriteBufferSize();
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        tTable.setWriteBufferSize(writeBufferSize);
    }

    @Override
    public void flushCommits() throws IOException {
        tTable.flushCommits();
    }

    @Override
    public void close() throws IOException {
        tTable.close();
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount, boolean writeToWAL)
            throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Boolean[] exists(List<Get> gets) throws IOException {
            // TODO Auto-generated method stub
            return null;
    }

    @Override
    public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
        // TODO Auto-generated method stub
    }

    @Override
    public void setAutoFlushTo(boolean autoFlush) {
        tTable.setAutoFlush(autoFlush);
    }

    @Override
    public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TableName getName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean[] existsAll(List<Get> gets) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
    }

    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions,
            Object[] results, Callback<R> callback) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> actions,
            Callback<R> callback) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
            byte[] value, Put put) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, Put put) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
            byte[] value, Delete delete) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, Delete delete)
            throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public Result append(Append append) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
            byte[] qualifier, long amount, Durability durability)
            throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(
            Class<T> service, byte[] startKey, byte[] endKey,
            Call<T, R> callable) throws ServiceException, Throwable {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> service,
            byte[] startKey, byte[] endKey, Call<T, R> callable,
            Callback<R> callback) throws ServiceException, Throwable {
        // TODO Auto-generated method stub
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(
            MethodDescriptor methodDescriptor, Message request,
            byte[] startKey, byte[] endKey, R responsePrototype)
            throws ServiceException, Throwable {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <R extends Message> void batchCoprocessorService(
            MethodDescriptor methodDescriptor, Message request,
            byte[] startKey, byte[] endKey, R responsePrototype,
            Callback<R> callback) throws ServiceException, Throwable {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
            CompareOp compareOp, byte[] value, RowMutations mutation)
            throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getOperationTimeout() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getRpcTimeout() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setOperationTimeout(int arg0) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setRpcTimeout(int arg0) {
        // TODO Auto-generated method stub
        
    }

}
